package queue

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sluice/internal/job"
)

const (
	HeartbeatTTL     = 15 * time.Second
	processingPrefix = "processing:"
	heartbeatPrefix  = "heartbeat:"
)

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

func NewClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}

// Enqueue pushes a job to the tenant-scoped priority queue.
func (q *Queue) Enqueue(ctx context.Context, tenantID uuid.UUID, jobID uuid.UUID, priority int16) error {
	key := bucketForPriority(priority) + ":" + tenantID.String()
	if err := q.rdb.LPush(ctx, key, jobID.String()).Err(); err != nil {
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
	if len(tenants) == 0 {
		select {
		case <-ctx.Done():
			return uuid.Nil, ctx.Err()
		case <-time.After(timeout):
			return uuid.Nil, nil
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
	_, vals, err := q.rdb.BLMPop(ctx, timeout, "right", 1, keys...).Result()
	if err == redis.Nil {
		return uuid.Nil, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return uuid.Nil, ctx.Err()
		}
		return uuid.Nil, fmt.Errorf("dequeue: %w", err)
	}
	id, err := uuid.Parse(vals[0])
	if err != nil {
		return uuid.Nil, fmt.Errorf("malformed job id %q in queue: %w", vals[0], err)
	}

	// The processing list only aids debugging: ownership and recovery are driven by
	// Postgres claims, and a job lost between the pop and this push is re-enqueued by
	// the scheduler's pending reconciler. The TTL stops lists of crashed workers
	// accumulating forever.
	dest := processingPrefix + workerID
	pipe := q.rdb.Pipeline()
	pipe.LPush(ctx, dest, id.String())
	pipe.Expire(ctx, dest, processingTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("record processing list", "worker_id", workerID, "job_id", id, "err", err)
	}
	return id, nil
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
