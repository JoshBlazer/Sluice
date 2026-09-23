//go:build integration

package worker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/testutil"
	"github.com/sluice/internal/worker"
)

func jobState(t *testing.T, ctx context.Context, db *pgxpool.Pool, j *job.Job) job.State {
	t.Helper()
	got, err := storage.GetJob(ctx, db, j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return got.State
}

// startWorker runs a worker against the test stack and returns it with a
// function that cancels its run context (like SIGTERM does in main).
func startWorker(t *testing.T, db *pgxpool.Pool, q *queue.Queue) (*worker.Worker, context.CancelFunc) {
	t.Helper()
	w := worker.New(db, q, worker.Options{AllowPrivateWebhooks: true})
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)
	return w, cancel
}

func TestWorker_ExecutesWebhook(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()

	w, cancel := startWorker(t, db, q)
	defer func() { cancel(); w.Shutdown(5 * time.Second) }()

	j := testutil.InsertJob(t, db, tn.ID, srv.URL, nil)
	if err := q.Enqueue(ctx, tn.ID, j.ID, j.Priority); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 10*time.Second, "job succeeded", func() bool {
		return jobState(t, ctx, db, j) == job.StateSucceeded
	})
	if hits.Load() != 1 {
		t.Fatalf("webhook called %d times, want 1", hits.Load())
	}
}

func TestWorker_FailureSchedulesRetry(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	w, cancel := startWorker(t, db, q)
	defer func() { cancel(); w.Shutdown(5 * time.Second) }()

	j := testutil.InsertJob(t, db, tn.ID, srv.URL, func(j *job.Job) { j.BackoffSeconds = 10 })
	q.Enqueue(ctx, tn.ID, j.ID, j.Priority)

	testutil.Eventually(t, 10*time.Second, "job failed", func() bool {
		return jobState(t, ctx, db, j) == job.StateFailed
	})
	got, _ := storage.GetJob(ctx, db, j.ID)
	// First retry waits the base backoff (10s ±20% jitter), not double it.
	wait := time.Until(got.RunAt)
	if wait < 7*time.Second || wait > 12*time.Second {
		t.Fatalf("retry scheduled %v from now, want ~10s", wait)
	}
	if got.Attempt != 1 || got.LastError == nil {
		t.Fatalf("attempt=%d last_error=%v, want 1 and an error", got.Attempt, got.LastError)
	}
}

func TestWorker_ShutdownDrainsInFlightJob(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(1500 * time.Millisecond)
	}))
	defer srv.Close()

	w, cancel := startWorker(t, db, q)
	j := testutil.InsertJob(t, db, tn.ID, srv.URL, nil)
	q.Enqueue(ctx, tn.ID, j.ID, j.Priority)

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("job never started")
	}

	// Simulate SIGTERM: cancel the run context, then drain.
	cancel()
	w.Shutdown(10 * time.Second)

	if s := jobState(t, ctx, db, j); s != job.StateSucceeded {
		t.Fatalf("state after graceful shutdown = %s, want succeeded (in-flight job must finish)", s)
	}
}

func TestWorker_ShutdownTimeoutAbortsAndRecords(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-r.Context().Done():
		case <-time.After(20 * time.Second):
		}
	}))
	defer srv.Close()

	w, cancel := startWorker(t, db, q)
	j := testutil.InsertJob(t, db, tn.ID, srv.URL, nil)
	q.Enqueue(ctx, tn.ID, j.ID, j.Priority)
	<-started

	cancel()
	begin := time.Now()
	w.Shutdown(500 * time.Millisecond)
	if took := time.Since(begin); took > 5*time.Second {
		t.Fatalf("shutdown took %v, want about the 500ms timeout", took)
	}

	// The aborted attempt is recorded straight away rather than left running
	// until the reaper notices.
	if s := jobState(t, ctx, db, j); s != job.StateFailed {
		t.Fatalf("state after aborted shutdown = %s, want failed (retry pending)", s)
	}
}
