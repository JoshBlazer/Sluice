package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/leader"
	"github.com/sluice/internal/metrics"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/storage"
)

const (
	duePollInterval          = 100 * time.Millisecond
	staleReapInterval        = 5 * time.Second
	deadLetterInterval       = 30 * time.Second
	cronInterval             = 60 * time.Second
	pendingReconcileInterval = 15 * time.Second
	// A pending job is only re-enqueued once it has been due this long, so the
	// API's own insert-then-enqueue isn't raced.
	pendingReconcileAge = 30 * time.Second
	// Cap per pass, so a huge backlog of genuinely queued jobs (for which
	// re-enqueueing is a no-op) doesn't turn into a flood of Redis calls.
	pendingReconcileMaxPerPass = 5000
	deadWorkerInterval         = 5 * time.Second
	partitionMaintainInterval  = 24 * time.Hour
	duePollBatchSize           = 500
	failedPollBatchSize        = 500
	cronBatchSize              = 200
	pendingReconcileBatchSize  = 500
	partitionLookaheadMonths   = 3
	deadLetterBatchSize        = 500
	queueDepthInterval         = 5 * time.Second
	retentionInterval          = time.Hour
	retentionBatchSize         = 1000
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

type Scheduler struct {
	db    *pgxpool.Pool
	queue *queue.Queue

	// Retention is how long finished jobs, their run history and dead-letter
	// entries are kept. Zero keeps everything.
	Retention time.Duration
}

func New(db *pgxpool.Pool, q *queue.Queue) *Scheduler {
	return &Scheduler{db: db, queue: q}
}

// Run enters the leader-election loop. It blocks until ctx is cancelled.
// The four scheduler loops only run while this instance holds the etcd lease.
// Non-leaders keep the DB connection warm and wait to take over.
func (s *Scheduler) Run(ctx context.Context, elect *leader.Election) {
	hostname := uuid.NewString() // unique identity within this election
	slog.Info("scheduler hot-standby — waiting for leader election")

	for ctx.Err() == nil {
		leaderCtx, resign, err := elect.Campaign(ctx, hostname)
		if err != nil {
			// ctx was cancelled during campaign — clean shutdown
			return
		}
		slog.Info("scheduler became leader")
		metrics.SchedulerIsLeader.Set(1)
		s.lead(leaderCtx)
		metrics.SchedulerIsLeader.Set(0)
		resign()
		slog.Info("scheduler lost leadership — re-entering election")
	}
}

// lead runs every scheduler loop until ctx (the leadership lease) ends.
func (s *Scheduler) lead(ctx context.Context) {
	var wg sync.WaitGroup
	for _, fn := range []func(context.Context){
		s.runDuePoll,
		s.runStaleReaper,
		s.runDeadLetterPromoter,
		s.runCronExpander,
		s.runPendingReconciler,
		s.runDeadWorkerRecovery,
		s.runPartitionMaintainer,
		s.runQueueDepthExporter,
		s.runRetention,
	} {
		wg.Add(1)
		go func(f func(context.Context)) {
			defer wg.Done()
			f(ctx)
		}(fn)
	}
	wg.Wait()
}

func (s *Scheduler) runDuePoll(ctx context.Context) {
	ticker := time.NewTicker(duePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pollScheduledJobs(ctx)
			s.pollFailedJobs(ctx)
		}
	}
}

func (s *Scheduler) pollScheduledJobs(ctx context.Context) {
	jobs, err := storage.GetDueJobs(ctx, s.db, duePollBatchSize)
	if err != nil {
		slog.Error("poll scheduled jobs", "err", err)
		return
	}
	for _, j := range jobs {
		if err := storage.PromoteScheduledToPending(ctx, s.db, j.ID); err != nil {
			slog.Error("promote scheduled job", "job_id", j.ID, "err", err)
			continue
		}
		if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
			slog.Error("enqueue scheduled job", "job_id", j.ID, "err", err)
		}
	}
}

func (s *Scheduler) pollFailedJobs(ctx context.Context) {
	jobs, err := storage.GetFailedReadyJobs(ctx, s.db, failedPollBatchSize)
	if err != nil {
		slog.Error("poll failed ready jobs", "err", err)
		return
	}
	for _, j := range jobs {
		if err := storage.PromoteFailedToPending(ctx, s.db, j.ID); err != nil {
			slog.Error("promote failed job", "job_id", j.ID, "err", err)
			continue
		}
		if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
			slog.Error("enqueue retried job", "job_id", j.ID, "err", err)
		}
	}
}

func (s *Scheduler) runStaleReaper(ctx context.Context) {
	ticker := time.NewTicker(staleReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reapStaleClaims(ctx)
		}
	}
}

func (s *Scheduler) reapStaleClaims(ctx context.Context) {
	jobs, err := storage.GetStaleClaims(ctx, s.db)
	if err != nil {
		slog.Error("get stale claims", "err", err)
		return
	}
	for _, j := range jobs {
		slog.Warn("reaping stale job", "job_id", j.ID, "claimed_by", j.ClaimedBy)
		dead, err := storage.RequeueStaleJob(ctx, s.db, j.ID)
		if err != nil {
			slog.Error("requeue stale job", "job_id", j.ID, "err", err)
			continue
		}
		if !dead {
			if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
				slog.Error("enqueue reaped job", "job_id", j.ID, "err", err)
			}
		}
	}
}

func (s *Scheduler) runDeadLetterPromoter(ctx context.Context) {
	ticker := time.NewTicker(deadLetterInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.promoteDeadJobs(ctx)
		}
	}
}

func (s *Scheduler) promoteDeadJobs(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := storage.MoveToDeadLetter(ctx, s.db, deadLetterBatchSize)
		if err != nil {
			slog.Error("move to dead letter", "err", err)
			return
		}
		if n > 0 {
			slog.Info("moved jobs to dead letter", "count", n)
		}
		if n < deadLetterBatchSize {
			return
		}
	}
}

func (s *Scheduler) runQueueDepthExporter(ctx context.Context) {
	defer metrics.QueueDepth.Reset()
	ticker := time.NewTicker(queueDepthInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.exportQueueDepths(ctx)
		}
	}
}

// exportQueueDepths publishes sluice_queue_depth. Only the leader exports it, so
// sum(sluice_queue_depth) across all scheduler pods is the true total.
func (s *Scheduler) exportQueueDepths(ctx context.Context) {
	tenants, err := storage.GetTenants(ctx, s.db)
	if err != nil {
		slog.Error("queue depth: load tenants", "err", err)
		return
	}
	ids := make([]uuid.UUID, len(tenants))
	for i, t := range tenants {
		ids[i] = t.ID
	}
	depths, err := s.queue.Depths(ctx, ids)
	if err != nil {
		slog.Error("queue depth: read redis", "err", err)
		return
	}
	metrics.QueueDepth.Reset()
	for _, d := range depths {
		metrics.QueueDepth.WithLabelValues(d.Priority, d.TenantID.String()).Set(float64(d.Depth))
	}
}

func (s *Scheduler) runRetention(ctx context.Context) {
	if s.Retention <= 0 {
		return
	}
	s.enforceRetention(ctx)
	ticker := time.NewTicker(retentionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.enforceRetention(ctx)
		}
	}
}

// enforceRetention deletes history older than Retention in small batches, so a
// large backlog never holds long locks or one huge transaction.
func (s *Scheduler) enforceRetention(ctx context.Context) {
	cutoff := time.Now().Add(-s.Retention)

	for _, prune := range []struct {
		what string
		fn   func(context.Context, *pgxpool.Pool, time.Time, int) (int64, error)
	}{
		{"finished jobs", storage.PruneFinishedJobs},
		{"dead-letter entries", storage.PruneDeadLetter},
	} {
		var total int64
		for ctx.Err() == nil {
			n, err := prune.fn(ctx, s.db, cutoff, retentionBatchSize)
			if err != nil {
				slog.Error("retention: prune "+prune.what, "err", err)
				break
			}
			total += n
			if n < retentionBatchSize {
				break
			}
		}
		if total > 0 {
			slog.Info("retention: pruned "+prune.what, "count", total, "older_than", cutoff)
		}
	}

	dropped, err := storage.DropJobRunsPartitionsBefore(ctx, s.db, cutoff)
	if err != nil {
		slog.Error("retention: drop job_runs partitions", "err", err)
	}
	if len(dropped) > 0 {
		slog.Info("retention: dropped job_runs partitions", "partitions", dropped)
	}
}

func (s *Scheduler) runPendingReconciler(ctx context.Context) {
	ticker := time.NewTicker(pendingReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reconcilePending(ctx)
		}
	}
}

// reconcilePending re-enqueues pending jobs that have been due a while, in case
// Redis lost them (flush, failover, drain). Enqueue is idempotent, so jobs that
// are in fact still queued are untouched.
func (s *Scheduler) reconcilePending(ctx context.Context) {
	var cursor storage.PendingCursor
	checked := 0
	for checked < pendingReconcileMaxPerPass && ctx.Err() == nil {
		jobs, next, err := storage.GetPendingJobs(ctx, s.db, pendingReconcileAge, cursor, pendingReconcileBatchSize)
		if err != nil {
			slog.Error("reconcile pending jobs", "err", err)
			return
		}
		for _, j := range jobs {
			if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
				slog.Error("re-enqueue pending job", "job_id", j.ID, "err", err)
			}
		}
		checked += len(jobs)
		if len(jobs) < pendingReconcileBatchSize {
			break
		}
		cursor = next
	}
	if checked > 0 {
		slog.Debug("reconciled pending jobs", "checked", checked)
	}
}

func (s *Scheduler) runDeadWorkerRecovery(ctx context.Context) {
	ticker := time.NewTicker(deadWorkerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recoverDeadWorkerJobs(ctx)
		}
	}
}

// recoverDeadWorkerJobs re-enqueues jobs a dead worker popped but never claimed.
// Jobs it did claim are running in Postgres and come back via the stale-claim
// reaper instead.
func (s *Scheduler) recoverDeadWorkerJobs(ctx context.Context) {
	ids, err := s.queue.DeadWorkerJobs(ctx)
	if err != nil {
		slog.Error("find dead workers' jobs", "err", err)
	}
	recovered := 0
	for _, id := range ids {
		j, err := storage.GetJob(ctx, s.db, id)
		if err != nil || j.State != job.StatePending {
			continue
		}
		if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
			slog.Error("re-enqueue dead worker's job", "job_id", id, "err", err)
			continue
		}
		recovered++
	}
	if recovered > 0 {
		slog.Warn("recovered jobs from dead workers", "count", recovered)
	}
}

func (s *Scheduler) runPartitionMaintainer(ctx context.Context) {
	s.ensurePartitions(ctx)

	ticker := time.NewTicker(partitionMaintainInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ensurePartitions(ctx)
		}
	}
}

func (s *Scheduler) ensurePartitions(ctx context.Context) {
	now := time.Now().UTC()
	for i := 1; i <= partitionLookaheadMonths; i++ {
		target := now.AddDate(0, i, 0)
		if err := storage.EnsureJobRunsPartition(ctx, s.db, target.Year(), target.Month()); err != nil {
			slog.Error("ensure job_runs partition", "year", target.Year(), "month", int(target.Month()), "err", err)
		} else {
			slog.Debug("job_runs partition ok", "year", target.Year(), "month", int(target.Month()))
		}
	}
}

func (s *Scheduler) runCronExpander(ctx context.Context) {
	s.expandCron(ctx)

	ticker := time.NewTicker(cronInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.expandCron(ctx)
		}
	}
}

func (s *Scheduler) expandCron(ctx context.Context) {
	schedules, err := storage.GetDueSchedules(ctx, s.db, cronBatchSize)
	if err != nil {
		slog.Error("get due schedules", "err", err)
		return
	}
	for _, sched := range schedules {
		if err := s.fireSchedule(ctx, sched); err != nil {
			slog.Error("fire schedule", "schedule_id", sched.ID, "name", sched.Name, "err", err)
		}
	}
}

func (s *Scheduler) fireSchedule(ctx context.Context, sched *storage.Schedule) error {
	expr, err := cronParser.Parse(sched.Cron)
	if err != nil {
		return fmt.Errorf("parse cron %q: %w", sched.Cron, err)
	}
	loc, err := time.LoadLocation(sched.Timezone)
	if err != nil {
		return fmt.Errorf("load timezone %q: %w", sched.Timezone, err)
	}

	now := time.Now()
	nextRunAt := expr.Next(now.In(loc))

	var template job.Template
	if err := json.Unmarshal(sched.JobTemplate, &template); err != nil {
		return fmt.Errorf("unmarshal job template: %w", err)
	}
	j, err := template.Build(sched.TenantID, now)
	if err != nil {
		return fmt.Errorf("invalid job template: %w", err)
	}
	// Keyed on the occurrence being fired, so if updating next_run_at below fails
	// and the schedule is picked up again, the duplicate insert is a no-op.
	key := fmt.Sprintf("schedule:%s:%d", sched.ID, sched.NextRunAt.Unix())
	j.IdempotencyKey = &key

	switch err := storage.InsertJob(ctx, s.db, j); {
	case errors.Is(err, storage.ErrDuplicate):
		slog.Info("cron occurrence already fired", "schedule", sched.Name, "occurrence", sched.NextRunAt)
	case err != nil:
		return fmt.Errorf("insert cron job: %w", err)
	default:
		if err := s.queue.Enqueue(ctx, j.TenantID, j.ID, j.Priority); err != nil {
			slog.Warn("enqueue cron job failed — job is durable in postgres", "job_id", j.ID, "err", err)
		}
		slog.Info("fired cron schedule", "schedule", sched.Name, "job_id", j.ID, "next_run_at", nextRunAt)
	}
	if err := storage.UpdateScheduleAfterRun(ctx, s.db, sched.ID, now, nextRunAt); err != nil {
		return fmt.Errorf("update schedule next_run_at: %w", err)
	}
	return nil
}
