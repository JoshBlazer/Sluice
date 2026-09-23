package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/metrics"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	dequeueTimeout    = time.Second
	heartbeatInterval = 5 * time.Second
	tenantRefresh     = 60 * time.Second
	recordTimeout     = 10 * time.Second
)

type Options struct {
	// AllowPrivateWebhooks lets webhook jobs reach loopback and private-network
	// addresses. Only for local development and tests.
	AllowPrivateWebhooks bool
}

type Worker struct {
	id       string
	db       *pgxpool.Pool
	queue    *queue.Queue
	http     *http.Client
	shutdown chan struct{}
	done     chan struct{}
	reload   chan struct{}

	// execCtx scopes job execution. It is independent of Run's ctx so a SIGTERM
	// drains in-flight work; abort cancels it once the drain timeout expires.
	execCtx context.Context
	abort   context.CancelFunc

	tenantsMu sync.RWMutex
	tenants   []queue.TenantWeight
}

func New(db *pgxpool.Pool, q *queue.Queue, opts Options) *Worker {
	execCtx, abort := context.WithCancel(context.Background())
	return &Worker{
		id:       uuid.NewString(),
		db:       db,
		queue:    q,
		http:     newWebhookClient(opts.AllowPrivateWebhooks),
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
		reload:   make(chan struct{}, 1),
		execCtx:  execCtx,
		abort:    abort,
	}
}

// Reload signals the worker to refresh its tenant weight cache from Postgres.
// Called on SIGHUP for hot config reload. Non-blocking.
func (w *Worker) Reload() {
	select {
	case w.reload <- struct{}{}:
	default:
	}
}

// Run pulls and executes jobs until Shutdown is called or ctx is cancelled.
// Cancelling ctx stops dequeuing but does not interrupt a running job; only
// Shutdown's timeout does that, so a SIGTERM lets in-flight work finish.
func (w *Worker) Run(ctx context.Context) {
	defer close(w.done)
	slog.Info("worker started", "worker_id", w.id)

	pollCtx, stopPolling := context.WithCancel(ctx)
	defer stopPolling()
	go func() {
		select {
		case <-w.shutdown:
			stopPolling()
		case <-pollCtx.Done():
		}
	}()

	w.loadTenants(pollCtx)

	// Background goroutine refreshes the tenant list periodically and on demand.
	go func() {
		ticker := time.NewTicker(tenantRefresh)
		defer ticker.Stop()
		for {
			select {
			case <-pollCtx.Done():
				return
			case <-ticker.C:
				w.loadTenants(pollCtx)
			case <-w.reload:
				slog.Info("worker reloading tenant weights", "worker_id", w.id)
				w.loadTenants(pollCtx)
			}
		}
	}()

	for pollCtx.Err() == nil {
		w.tenantsMu.RLock()
		tenants := w.tenants
		w.tenantsMu.RUnlock()

		jobID, err := w.queue.Dequeue(pollCtx, w.id, tenants, dequeueTimeout)
		if err != nil {
			if pollCtx.Err() != nil {
				break
			}
			slog.Error("dequeue failed", "err", err)
			// Redis is unreachable or erroring; back off rather than spin.
			select {
			case <-pollCtx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		if jobID == uuid.Nil {
			continue
		}

		w.process(w.execCtx, jobID)
	}
	slog.Info("worker stopped", "worker_id", w.id)
}

// Shutdown stops dequeuing and waits up to timeout for the in-flight job to
// finish. If it doesn't, the job is aborted and recorded as a failed attempt.
func (w *Worker) Shutdown(timeout time.Duration) {
	close(w.shutdown)
	select {
	case <-w.done:
		return
	case <-time.After(timeout):
	}
	slog.Warn("worker shutdown timeout — aborting in-flight job")
	w.abort()
	select {
	case <-w.done:
	case <-time.After(recordTimeout):
		slog.Warn("in-flight job did not stop — the scheduler will requeue it after its deadline")
	}
}

func (w *Worker) loadTenants(ctx context.Context) {
	tenants, err := storage.GetTenants(ctx, w.db)
	if err != nil {
		slog.Error("load tenant weights", "err", err)
		return
	}
	weights := make([]queue.TenantWeight, len(tenants))
	for i, t := range tenants {
		w := t.Weight
		if w <= 0 {
			w = 100
		}
		weights[i] = queue.TenantWeight{ID: t.ID, Weight: w}
	}
	w.tenantsMu.Lock()
	w.tenants = weights
	w.tenantsMu.Unlock()
	slog.Info("tenant weights loaded", "count", len(weights))
}

func (w *Worker) process(ctx context.Context, jobID uuid.UUID) {
	tracer := telemetry.Tracer("sluice/worker")
	ctx, span := tracer.Start(ctx, "worker.execute")
	span.SetAttributes(attribute.String("job.id", jobID.String()))
	defer span.End()
	defer w.queue.RemoveFromProcessing(context.WithoutCancel(ctx), w.id, jobID) //nolint:errcheck

	token := uuid.New()
	// The first heartbeat extends this; a worker that dies before sending one is
	// reaped within HeartbeatTTL plus one reaper interval.
	deadline := time.Now().Add(queue.HeartbeatTTL)

	ok, runID, err := storage.TryClaim(ctx, w.db, jobID, w.id, token, deadline)
	if err != nil {
		telemetry.L(ctx).Error("claim failed", "job_id", jobID, "err", err)
		span.RecordError(err)
		return
	}
	if !ok {
		return
	}

	j, err := storage.GetJob(ctx, w.db, jobID)
	if err != nil {
		// The claim stands; the stale-claim reaper requeues the job once its deadline passes.
		telemetry.L(ctx).Error("get job after claim", "job_id", jobID, "err", err)
		span.RecordError(err)
		return
	}

	span.SetAttributes(
		attribute.String("job.type", j.Type),
		attribute.String("job.tenant_id", j.TenantID.String()),
		attribute.Int("job.attempt", j.Attempt),
	)

	if err := storage.MarkRunning(ctx, w.db, jobID, token); err != nil {
		telemetry.L(ctx).Warn("mark running", "job_id", jobID, "err", err)
	}

	metrics.WorkerInFlight.Inc()
	hbCtx, hbCancel := context.WithCancel(ctx)
	go w.heartbeat(hbCtx, jobID, token)

	telemetry.L(ctx).Info("executing job", "job_id", jobID, "type", j.Type, "attempt", j.Attempt, "tenant_id", j.TenantID)
	start := time.Now()
	execErr := w.execute(ctx, j)
	dur := time.Since(start)

	hbCancel()
	metrics.WorkerInFlight.Dec()

	// Record the outcome even if execution was aborted by shutdown.
	recCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
	defer cancel()

	tenantID := j.TenantID.String()

	if execErr != nil {
		telemetry.L(ctx).Warn("job failed", "job_id", jobID, "err", execErr, "tenant_id", tenantID)
		span.RecordError(execErr)
		span.SetStatus(codes.Error, execErr.Error())

		var nextRunAt *time.Time
		finalState := "dead"
		if j.ShouldRetry() {
			t := job.NextRetryAt(j.Attempt, j.BackoffSeconds, time.Now())
			nextRunAt = &t
			finalState = "failed"
			metrics.JobRetriesTotal.WithLabelValues(j.Type, tenantID).Inc()
		}
		metrics.JobsTotal.WithLabelValues(j.Type, finalState, tenantID).Inc()
		metrics.JobDurationSeconds.WithLabelValues(j.Type, finalState, tenantID).Observe(dur.Seconds())

		if err := storage.FailJob(recCtx, w.db, jobID, runID, token, execErr.Error(), nextRunAt); err != nil {
			telemetry.L(ctx).Error("record job failure", "job_id", jobID, "err", err)
		}
	} else {
		telemetry.L(ctx).Info("job succeeded", "job_id", jobID, "tenant_id", tenantID, "duration_ms", dur.Milliseconds())
		span.SetStatus(codes.Ok, "")
		metrics.JobsTotal.WithLabelValues(j.Type, "succeeded", tenantID).Inc()
		metrics.JobDurationSeconds.WithLabelValues(j.Type, "succeeded", tenantID).Observe(dur.Seconds())
		if err := storage.CompleteJob(recCtx, w.db, jobID, runID, token); err != nil {
			telemetry.L(ctx).Error("record job success", "job_id", jobID, "err", err)
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context, jobID uuid.UUID, token uuid.UUID) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.queue.Heartbeat(ctx, jobID, token.String()); err != nil {
				slog.Warn("heartbeat failed", "job_id", jobID, "err", err)
			}
			// Extend the Postgres deadline so the stale-claim reaper doesn't reassign
			// a healthy job. TTL matches queue.HeartbeatTTL: 3 missed beats = reassignment.
			newDeadline := time.Now().Add(queue.HeartbeatTTL)
			if err := storage.ExtendDeadline(ctx, w.db, jobID, token, newDeadline); err != nil {
				slog.Warn("extend deadline failed", "job_id", jobID, "err", err)
			}
		}
	}
}

func (w *Worker) execute(ctx context.Context, j *job.Job) error {
	switch j.Type {
	case "webhook":
		return w.executeWebhook(ctx, j)
	default:
		return fmt.Errorf("unknown job type: %s", j.Type)
	}
}
