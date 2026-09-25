//go:build integration

package worker_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestWorker_RunsJobsConcurrently(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	const concurrency, jobs = 4, 8
	var running, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
		running.Add(-1)
	}))
	defer srv.Close()

	w := worker.New(db, q, worker.Options{Concurrency: concurrency, AllowPrivateWebhooks: true})
	runCtx, cancel := context.WithCancel(context.Background())
	go w.Run(runCtx)
	defer func() { cancel(); w.Shutdown(5 * time.Second) }()

	var ids []*job.Job
	for i := 0; i < jobs; i++ {
		j := testutil.InsertJob(t, db, tn.ID, srv.URL, nil)
		q.Enqueue(ctx, tn.ID, j.ID, j.Priority)
		ids = append(ids, j)
	}
	start := time.Now()
	testutil.Eventually(t, 15*time.Second, "all jobs succeeded", func() bool {
		for _, j := range ids {
			if jobState(t, ctx, db, j) != job.StateSucceeded {
				return false
			}
		}
		return true
	})
	// 8 jobs × 500ms at concurrency 4 is ~1s; serially it would be 4s.
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %v, jobs don't seem to run concurrently", took)
	}
	if p := peak.Load(); p != concurrency {
		t.Fatalf("peak concurrent executions = %d, want %d", p, concurrency)
	}
}

func TestWorker_ShutdownDrainsAllInFlightJobs(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	var started atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Add(1)
		time.Sleep(time.Second)
	}))
	defer srv.Close()

	w := worker.New(db, q, worker.Options{Concurrency: 3, AllowPrivateWebhooks: true})
	runCtx, cancel := context.WithCancel(context.Background())
	go w.Run(runCtx)

	var ids []*job.Job
	for i := 0; i < 3; i++ {
		j := testutil.InsertJob(t, db, tn.ID, srv.URL, nil)
		q.Enqueue(ctx, tn.ID, j.ID, j.Priority)
		ids = append(ids, j)
	}
	testutil.Eventually(t, 10*time.Second, "all three jobs started", func() bool { return started.Load() == 3 })

	cancel()
	w.Shutdown(10 * time.Second)
	for _, j := range ids {
		if s := jobState(t, ctx, db, j); s != job.StateSucceeded {
			t.Fatalf("job %s = %s after graceful shutdown, want succeeded", j.ID, s)
		}
	}
}

// A receiver holding the tenant's secret (as issued by create-tenant or
// GET /v1/webhook-secret) can verify the request end to end.
func TestWorker_SignsWithTenantSecret(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))
	tn, _ := testutil.Tenant(t, db, 0, 100)

	type delivery struct {
		id, ts, sig string
		body        []byte
	}
	got := make(chan delivery, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got <- delivery{r.Header.Get("webhook-id"), r.Header.Get("webhook-timestamp"), r.Header.Get("webhook-signature"), b}
	}))
	defer srv.Close()

	w, cancel := startWorker(t, db, q)
	defer func() { cancel(); w.Shutdown(5 * time.Second) }()

	j := testutil.InsertJob(t, db, tn.ID, srv.URL, func(j *job.Job) {
		j.Payload = []byte(`{"url":"` + srv.URL + `","body":{"hello":"world"}}`)
	})
	q.Enqueue(ctx, tn.ID, j.ID, j.Priority)

	var d delivery
	select {
	case d = <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("webhook never arrived")
	}
	key, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(tn.WebhookSecret, "whsec_"))
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(d.id + "." + d.ts + "."))
	mac.Write(d.body)
	if want := "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil)); d.sig != want {
		t.Fatalf("signature %q does not verify with the tenant secret (want %q)", d.sig, want)
	}
	if d.id != j.ID.String() {
		t.Fatalf("webhook-id = %q, want job ID %s", d.id, j.ID)
	}
}

// A tenant created after the worker started must not wait long for its jobs:
// workers only dequeue for tenants they know about.
func TestWorker_PicksUpNewTenantQuickly(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	q := queue.New(testutil.Redis(t))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	w, cancel := startWorker(t, db, q)
	defer func() { cancel(); w.Shutdown(5 * time.Second) }()
	time.Sleep(500 * time.Millisecond) // worker has loaded its tenant list

	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, srv.URL, nil)
	q.Enqueue(ctx, tn.ID, j.ID, j.Priority)

	start := time.Now()
	testutil.Eventually(t, 15*time.Second, "new tenant's job succeeded", func() bool {
		return jobState(t, ctx, db, j) == job.StateSucceeded
	})
	if took := time.Since(start); took > 8*time.Second {
		t.Fatalf("new tenant's job took %v to run, want well under 10s", took)
	}
}
