//go:build integration

package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/testutil"
)

// failOnce claims j and records one failed attempt, returning the resulting state.
func failOnce(t *testing.T, ctx context.Context, db *pgxpool.Pool, j *job.Job) job.State {
	t.Helper()
	token := uuid.New()
	ok, runID, err := storage.TryClaim(ctx, db, j.ID, "w", token, time.Now().Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	cur, err := storage.GetJob(ctx, db, j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	var next *time.Time
	if cur.ShouldRetry() {
		n := time.Now().Add(-time.Second)
		next = &n
	}
	if err := storage.FailJob(ctx, db, j.ID, runID, token, "boom", next); err != nil {
		t.Fatalf("fail job: %v", err)
	}
	after, err := storage.GetJob(ctx, db, j.ID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if after.State == job.StateFailed {
		if err := storage.PromoteFailedToPending(ctx, db, j.ID); err != nil {
			t.Fatalf("promote: %v", err)
		}
	}
	return after.State
}

func TestFailJob_MaxRetriesMeansRetries(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.MaxRetries = 3 })

	// max_retries=3: the first attempt plus 3 retries = 4 executions before dead.
	for i := 1; i <= 3; i++ {
		if got := failOnce(t, ctx, db, j); got != job.StateFailed {
			t.Fatalf("attempt %d: state = %s, want failed", i, got)
		}
	}
	if got := failOnce(t, ctx, db, j); got != job.StateDead {
		t.Fatalf("attempt 4: state = %s, want dead", got)
	}
}

func TestFailJob_ZeroRetries(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.MaxRetries = 0 })

	if got := failOnce(t, ctx, db, j); got != job.StateDead {
		t.Fatalf("state = %s, want dead after the only attempt", got)
	}
}

func TestCompleteJob_StaleTokenIsDiscarded(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, "https://example.com", nil)

	ok, runID, err := storage.TryClaim(ctx, db, j.ID, "w", uuid.New(), time.Now().Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if err := storage.CompleteJob(ctx, db, j.ID, runID, uuid.New()); err != nil {
		t.Fatalf("complete with stale token should be a silent no-op, got %v", err)
	}
	got, _ := storage.GetJob(ctx, db, j.ID)
	if got.State != job.StateClaimed {
		t.Fatalf("state = %s, want claimed (stale completion must not apply)", got.State)
	}
}

func TestCompleteJob_RecordsSubSecondDuration(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, "https://example.com", nil)

	token := uuid.New()
	ok, runID, err := storage.TryClaim(ctx, db, j.ID, "w", token, time.Now().Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	time.Sleep(250 * time.Millisecond)
	if err := storage.CompleteJob(ctx, db, j.ID, runID, token); err != nil {
		t.Fatalf("complete: %v", err)
	}
	var ms int
	if err := db.QueryRow(ctx, `SELECT duration_ms FROM job_runs WHERE id = $1`, runID).Scan(&ms); err != nil {
		t.Fatalf("read run: %v", err)
	}
	if ms < 200 || ms > 2000 {
		t.Fatalf("duration_ms = %d, want ~250", ms)
	}
}

func TestRequeueStaleJob(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.MaxRetries = 1 })

	claimExpired := func() {
		ok, _, err := storage.TryClaim(ctx, db, j.ID, "w", uuid.New(), time.Now().Add(-time.Second))
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
	}

	claimExpired()
	dead, err := storage.RequeueStaleJob(ctx, db, j.ID)
	if err != nil || dead {
		t.Fatalf("first reap: dead=%v err=%v, want requeued", dead, err)
	}
	claimExpired()
	dead, err = storage.RequeueStaleJob(ctx, db, j.ID)
	if err != nil || !dead {
		t.Fatalf("second reap: dead=%v err=%v, want dead (max_retries=1)", dead, err)
	}
}

func TestMoveToDeadLetter_ReachesOldJobsBeyondBatch(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)

	const n = 7
	for i := 0; i < n; i++ {
		testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) {
			j.State = job.StateDead
			j.CreatedAt = time.Now().Add(-time.Duration(i) * time.Hour)
		})
	}

	// Batches smaller than the backlog must still drain it completely.
	for {
		moved, err := storage.MoveToDeadLetter(ctx, db, 3)
		if err != nil {
			t.Fatalf("move: %v", err)
		}
		if moved < 3 {
			break
		}
	}
	entries, err := storage.ListDeadLetter(ctx, db, tn.ID, 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("dead_letter has %d entries for tenant, want %d", len(entries), n)
	}
}

func TestReplayJob_ClearsDeadLetter(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.State = job.StateDead })
	if _, err := storage.MoveToDeadLetter(ctx, db, 1000); err != nil {
		t.Fatalf("move: %v", err)
	}

	other, _ := testutil.Tenant(t, db, 0, 100)
	if _, err := storage.ReplayJob(ctx, db, j.ID, other.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("replay by another tenant: err = %v, want ErrNotFound", err)
	}

	replayed, err := storage.ReplayJob(ctx, db, j.ID, tn.ID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.State != job.StatePending || replayed.Attempt != 0 {
		t.Fatalf("replayed job = %s/attempt %d, want pending/0", replayed.State, replayed.Attempt)
	}
	entries, _ := storage.ListDeadLetter(ctx, db, tn.ID, 10)
	if len(entries) != 0 {
		t.Fatalf("dead_letter still has %d entries after replay", len(entries))
	}
}

func TestCancelJob(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	j := testutil.InsertJob(t, db, tn.ID, "https://example.com", nil)

	if err := storage.CancelJob(ctx, db, j.ID, uuid.New()); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cancel by wrong tenant: err = %v, want ErrNotFound", err)
	}
	if err := storage.CancelJob(ctx, db, j.ID, tn.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	got, _ := storage.GetJob(ctx, db, j.ID)
	if got.State != job.StateCancelled {
		t.Fatalf("state = %s, want cancelled", got.State)
	}
	if ok, _, _ := storage.TryClaim(ctx, db, j.ID, "w", uuid.New(), time.Now().Add(time.Minute)); ok {
		t.Fatal("a cancelled job must not be claimable")
	}
	if err := storage.CancelJob(ctx, db, j.ID, tn.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("second cancel: err = %v, want ErrNotFound", err)
	}
}

func TestIdempotencyKeyIsPerTenant(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	a, _ := testutil.Tenant(t, db, 0, 100)
	b, _ := testutil.Tenant(t, db, 0, 100)
	key := "order-" + uuid.NewString()
	withKey := func(j *job.Job) { j.IdempotencyKey = &key }

	testutil.InsertJob(t, db, a.ID, "https://example.com", withKey)
	testutil.InsertJob(t, db, b.ID, "https://example.com", withKey) // other tenant: allowed

	dup := &job.Job{ID: uuid.New(), TenantID: a.ID, Type: "webhook", Payload: []byte(`{}`),
		Priority: 5, State: job.StatePending, RunAt: time.Now(), IdempotencyKey: &key, CreatedAt: time.Now()}
	if err := storage.InsertJob(ctx, db, dup); !errors.Is(err, storage.ErrDuplicate) {
		t.Fatalf("duplicate key: err = %v, want ErrDuplicate", err)
	}
}

func TestAPIKeys(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, key := testutil.Tenant(t, db, 0, 100)

	got, err := storage.GetTenantByAPIKey(ctx, db, key)
	if err != nil || got.ID != tn.ID {
		t.Fatalf("lookup by key: tenant=%v err=%v", got, err)
	}

	var stored string
	if err := db.QueryRow(ctx, `SELECT api_key_hash FROM tenants WHERE id = $1`, tn.ID).Scan(&stored); err != nil {
		t.Fatalf("read hash: %v", err)
	}
	if stored == key {
		t.Fatal("plaintext key stored in database")
	}

	newKey, err := storage.RotateAPIKey(ctx, db, tn.ID)
	if err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if _, err := storage.GetTenantByAPIKey(ctx, db, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old key after rotation: err = %v, want ErrNotFound", err)
	}
	if _, err := storage.GetTenantByAPIKey(ctx, db, newKey); err != nil {
		t.Fatalf("new key: %v", err)
	}
}
