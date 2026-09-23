//go:build integration

package scheduler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	dto "github.com/prometheus/client_model/go"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/metrics"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/testutil"
	"github.com/sluice/internal/worker"
)

func getJob(t *testing.T, db *pgxpool.Pool, id uuid.UUID) *job.Job {
	t.Helper()
	j, err := storage.GetJob(context.Background(), db, id)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return j
}

// startStack runs the scheduler loops (as leader) and one worker.
func startStack(t *testing.T, db *pgxpool.Pool, q *queue.Queue) *Scheduler {
	t.Helper()
	s := New(db, q)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.lead(ctx); close(done) }()

	w := worker.New(db, q, worker.Options{AllowPrivateWebhooks: true})
	wctx, wcancel := context.WithCancel(context.Background())
	go w.Run(wctx)

	t.Cleanup(func() {
		wcancel()
		w.Shutdown(5 * time.Second)
		cancel()
		<-done
	})
	return s
}

func TestRetriesThenDeadLetter(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	s := startStack(t, db, q)
	j := testutil.InsertJob(t, db, tn.ID, srv.URL, func(j *job.Job) {
		j.MaxRetries = 2
		j.BackoffSeconds = 1
	})
	q.Enqueue(ctx, tn.ID, j.ID, j.Priority)

	testutil.Eventually(t, 20*time.Second, "job dead", func() bool {
		return getJob(t, db, j.ID).State == job.StateDead
	})
	if got := calls.Load(); got != 3 {
		t.Fatalf("webhook called %d times, want 3 (1 attempt + max_retries=2)", got)
	}

	var runs int
	db.QueryRow(ctx, `SELECT COUNT(*) FROM job_runs WHERE job_id = $1`, j.ID).Scan(&runs)
	if runs != 3 {
		t.Fatalf("job_runs rows = %d, want 3", runs)
	}

	s.promoteDeadJobs(ctx)
	entries, err := storage.ListDeadLetter(ctx, db, tn.ID, 10)
	if err != nil || len(entries) != 1 || entries[0].JobID != j.ID {
		t.Fatalf("dead_letter entries = %v err=%v, want the job", entries, err)
	}
}

// A worker that claims a job and dies (no heartbeats, no completion) must not
// strand it: Phase 1 requires reassignment within 20 seconds.
func TestCrashedWorkerJobIsReassigned(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	j := testutil.InsertJob(t, db, tn.ID, srv.URL, nil)
	// The "crashed" worker: claims with the same initial deadline a real worker uses.
	ok, _, err := storage.TryClaim(ctx, db, j.ID, "dead-worker", uuid.New(), time.Now().Add(queue.HeartbeatTTL))
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	crashedAt := time.Now()

	startStack(t, db, q)
	testutil.Eventually(t, 25*time.Second, "job reassigned and completed", func() bool {
		return getJob(t, db, j.ID).State == job.StateSucceeded
	})
	if took := time.Since(crashedAt); took > 21*time.Second {
		t.Fatalf("recovery took %v, want <= 20s", took)
	}
	if got := getJob(t, db, j.ID); got.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1 (the crashed attempt counts)", got.Attempt)
	}
}

func TestFireSchedule_DedupesAndUsesTimezone(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)
	s := New(db, q)

	tmpl, _ := json.Marshal(map[string]any{"type": "webhook", "payload": map[string]any{"url": "https://example.com"}})
	sched := &storage.Schedule{
		ID: uuid.New(), TenantID: tn.ID, Name: "daily", Cron: "0 9 * * *",
		Timezone: "Asia/Tokyo", JobTemplate: tmpl, Enabled: true,
		NextRunAt: time.Now().Add(-time.Minute).Truncate(time.Second),
	}
	if err := storage.InsertSchedule(ctx, db, sched); err != nil {
		t.Fatal(err)
	}

	// Firing the same occurrence twice (e.g. the next_run_at update failed) creates one job.
	if err := s.fireSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	if err := s.fireSchedule(ctx, sched); err != nil {
		t.Fatal(err)
	}
	var n int
	db.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE tenant_id = $1`, tn.ID).Scan(&n)
	if n != 1 {
		t.Fatalf("jobs created = %d, want 1", n)
	}

	updated, err := storage.GetSchedule(ctx, db, sched.ID, tn.ID)
	if err != nil {
		t.Fatal(err)
	}
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	next := updated.NextRunAt.In(tokyo)
	if next.Hour() != 9 || next.Minute() != 0 {
		t.Fatalf("next_run_at = %v in Tokyo, want 09:00", next)
	}
}

func TestExportQueueDepths(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)
	s := New(db, q)

	for i := 0; i < 4; i++ {
		q.Enqueue(ctx, tn.ID, uuid.New(), job.PriorityHigh)
	}
	s.exportQueueDepths(ctx)
	var m dto.Metric
	if err := metrics.QueueDepth.WithLabelValues("high", tn.ID.String()).Write(&m); err != nil {
		t.Fatal(err)
	}
	if got := m.GetGauge().GetValue(); got != 4 {
		t.Fatalf("sluice_queue_depth{priority=high} = %v, want 4", got)
	}
}
