package queue

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sluice/internal/job"
)

const (
	HeartbeatTTL     = 15 * time.Second
	processingPrefix = "processing:"
	heartbeatPrefix  = "heartbeat:"
	// enqueued:{job_id} marks a job as waiting in some queue list.
	enqueuedPrefix = "enqueued:"
)

// EnqueuedPattern matches every job's "already queued" marker, for tools that
// flush the queues and must clear markers along with them.
const EnqueuedPattern = enqueuedPrefix + "*"

// Priority bucket names. Queue keys are "{bucket}:{tenantID}".
var priorityBuckets = []string{"queue:1", "queue:5", "queue:10"}

func bucketForPriority(priority int16) string {
	switch {
	case priority <= job.PriorityHigh:
		return "queue:1"
	case priority <= job.PriorityNormal:
		return "queue:5"
	default:
		return "queue:10"
	}
}

// TenantWeight pairs a tenant ID with its scheduling weight.
// Workers iterate tenants in proportion to their weight when dequeuing.
type TenantWeight struct {
	ID     uuid.UUID
	Weight int
}

type Queue struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Queue {
	return &Queue{rdb: rdb}
}

// NewClient connects to Redis. addr is either host:port or a URL carrying
// credentials, database and TLS: redis://[user:pass@]host:port[/db], or
// rediss://... for TLS, as managed Redis services issue them.
func NewClient(addr string) (*redis.Client, error) {
	if !strings.Contains(addr, "://") {
		return redis.NewClient(&redis.Options{Addr: addr}), nil
	}
	opts, err := redis.ParseURL(addr)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	return redis.NewClient(opts), nil
}

// EnqueuedTTL bounds how long a job's "already queued" marker lives. It is the
// longest a crash between a worker's pop and its marker delete can delay the
// scheduler's re-enqueue of that job, and, under a backlog older than this, the
// most often a still-waiting job can gain a (harmless) duplicate entry.
const EnqueuedTTL = 10 * time.Minute

// enqueueScript pushes the job only if its marker was newly set, so every
// re-enqueue path (API, scheduler, reconciler, reaper) is idempotent while the
// job is still waiting in Redis.
var enqueueScript = redis.NewScript(`
if redis.call('SET', KEYS[2], '1', 'NX', 'EX', ARGV[2]) then
  redis.call('LPUSH', KEYS[1], ARGV[1])
  return 1
end
return 0
`)

// Enqueue pushes a job to the tenant-scoped priority queue. It is a no-op if the
// job is already waiting there.
func (q *Queue) Enqueue(ctx context.Context, tenantID uuid.UUID, jobID uuid.UUID, priority int16) error {
	key := bucketForPriority(priority) + ":" + tenantID.String()
	err := enqueueScript.Run(ctx, q.rdb, []string{key, enqueuedPrefix + jobID.String()},
		jobID.String(), int(EnqueuedTTL.Seconds())).Err()
	if err != nil {
		return fmt.Errorf("enqueue job %s to %s: %w", jobID, key, err)
	}
	return nil
}

const processingTTL = time.Hour

// Dequeue pops the next job, blocking up to timeout for one to arrive. Priority lanes
// are strict: any tenant's high-priority job beats every normal one. Within a lane,
// tenants are visited in a weighted-random order so higher-weight tenants receive a
// proportionally larger share of worker time. Returns uuid.Nil with no error if
// nothing arrives before timeout.
func (q *Queue) Dequeue(ctx context.Context, workerID string, tenants []TenantWeight, timeout time.Duration) (uuid.UUID, error) {
	item, err := q.Pop(ctx, workerID, tenants, timeout)
	return item.JobID, err
}

// Item is a popped job and the list it came from, so it can be put back.
type Item struct {
	JobID uuid.UUID
	List  string
}

// Requeue puts a popped job back at the end of the line in the list it came from, e.g. when
// the worker couldn't claim it because Postgres was unavailable. Like Enqueue it
// is a no-op if the job is already waiting.
func (q *Queue) Requeue(ctx context.Context, item Item) error {
	err := enqueueScript.Run(ctx, q.rdb, []string{item.List, enqueuedPrefix + item.JobID.String()},
		item.JobID.String(), int(EnqueuedTTL.Seconds())).Err()
	if err != nil {
		return fmt.Errorf("requeue job %s to %s: %w", item.JobID, item.List, err)
	}
	return nil
}

// Pop is Dequeue, also reporting which list the job came from.
func (q *Queue) Pop(ctx context.Context, workerID string, tenants []TenantWeight, timeout time.Duration) (Item, error) {
	if len(tenants) == 0 {
		select {
		case <-ctx.Done():
			return Item{}, ctx.Err()
		case <-time.After(timeout):
			return Item{}, nil
		}
	}

	order := weightedShuffle(tenants)
	keys := make([]string, 0, len(priorityBuckets)*len(order))
	for _, bucket := range priorityBuckets {
		for _, tenantID := range order {
			keys = append(keys, bucket+":"+tenantID.String())
		}
	}

	// BLMPOP takes from the first non-empty key in order and wakes as soon as any
	// key receives a job, so pickup latency is a round trip, not a poll interval.
	list, vals, err := q.rdb.BLMPop(ctx, timeout, "right", 1, keys...).Result()
	if err == redis.Nil {
		return Item{}, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return Item{}, ctx.Err()
		}
		return Item{}, fmt.Errorf("dequeue: %w", err)
	}
	id, err := uuid.Parse(vals[0])
	if err != nil {
		return Item{}, fmt.Errorf("malformed job id %q in queue: %w", vals[0], err)
	}

	// Clearing the marker lets the job be enqueued again (e.g. after a failed claim or
	// a retry). The processing list only aids debugging: ownership and recovery are
	// driven by Postgres claims, and a job lost between the pop and this pipeline is
	// re-enqueued by the scheduler's pending reconciler once its marker expires. The
	// TTL stops lists of crashed workers accumulating forever.
	dest := processingPrefix + workerID
	pipe := q.rdb.Pipeline()
	pipe.Del(ctx, enqueuedPrefix+id.String())
	pipe.LPush(ctx, dest, id.String())
	pipe.Expire(ctx, dest, processingTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("record processing list", "worker_id", workerID, "job_id", id, "err", err)
	}
	return Item{JobID: id, List: list}, nil
}

// Depth is the number of jobs waiting in one tenant's priority lane.
type Depth struct {
	TenantID uuid.UUID `json:"tenant_id"`
	Priority string    `json:"priority"`
	Depth    int64     `json:"depth"`
}

var priorityLabels = map[string]string{"queue:1": "high", "queue:5": "normal", "queue:10": "low"}

// Depths returns the length of every priority lane for each tenant in one pipelined round trip.
func (q *Queue) Depths(ctx context.Context, tenantIDs []uuid.UUID) ([]Depth, error) {
	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, len(tenantIDs)*len(priorityBuckets))
	out := make([]Depth, 0, cap(cmds))
	for _, id := range tenantIDs {
		for _, bucket := range priorityBuckets {
			cmds = append(cmds, pipe.LLen(ctx, bucket+":"+id.String()))
			out = append(out, Depth{TenantID: id, Priority: priorityLabels[bucket]})
		}
	}
	if len(cmds) == 0 {
		return out, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("queue depths: %w", err)
	}
	for i, c := range cmds {
		out[i].Depth = c.Val()
	}
	return out, nil
}

// Heartbeat refreshes the TTL-keyed heartbeat for a running job.
func (q *Queue) Heartbeat(ctx context.Context, jobID uuid.UUID, token string) error {
	key := heartbeatPrefix + jobID.String()
	if err := q.rdb.Set(ctx, key, token, HeartbeatTTL).Err(); err != nil {
		return fmt.Errorf("heartbeat job %s: %w", jobID, err)
	}
	return nil
}

// RemoveFromProcessing clears the job from the worker's in-flight list after completion.
func (q *Queue) RemoveFromProcessing(ctx context.Context, workerID string, jobID uuid.UUID) error {
	key := processingPrefix + workerID
	if err := q.rdb.LRem(ctx, key, 0, jobID.String()).Err(); err != nil {
		return fmt.Errorf("remove from processing list: %w", err)
	}
	return nil
}

// weightedShuffle returns a weighted-random permutation of tenant IDs.
// Tenants with higher weights appear in more favourable positions on average.
func weightedShuffle(tenants []TenantWeight) []uuid.UUID {
	remaining := make([]TenantWeight, len(tenants))
	copy(remaining, tenants)

	out := make([]uuid.UUID, 0, len(tenants))
	for len(remaining) > 0 {
		total := 0
		for _, t := range remaining {
			total += t.Weight
		}
		r := rand.Intn(total)
		cum := 0
		for i, t := range remaining {
			cum += t.Weight
			if r < cum {
				out = append(out, t.ID)
				remaining = append(remaining[:i], remaining[i+1:]...)
				break
			}
		}
	}
	return out
}
