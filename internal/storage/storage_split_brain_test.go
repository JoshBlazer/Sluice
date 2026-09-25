//go:build integration

package storage_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/storage"
)

// TestTryClaim_SkipLocked verifies that concurrent schedulers cannot double-claim the same job.
// This test requires the Docker stack to be running.
// Run with: go test -tags integration ./internal/storage/... -run TestTryClaim_SkipLocked
func TestTryClaim_SkipLocked(t *testing.T) {
	const dsn = "postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable"
	ctx := context.Background()

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		t.Skipf("postgres not available: %v", err)
	}

	// Insert a fresh pending job.
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	j := &job.Job{
		ID:             uuid.New(),
		TenantID:       tenantID,
		Type:           "webhook",
		Payload:        []byte(`{"url":"http://example.com"}`),
		Priority:       5,
		State:          job.StatePending,
		RunAt:          time.Now(),
		Attempt:        0,
		MaxRetries:     3,
		BackoffSeconds: 30,
		CreatedAt:      time.Now(),
	}
	if err := storage.InsertJob(ctx, db, j); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	t.Cleanup(func() {
		db.Exec(ctx, "DELETE FROM jobs WHERE id = $1", j.ID)
	})

	// Launch N workers all trying to claim the same job simultaneously.
	const workers = 10
	var claims atomic.Int32
	var wg sync.WaitGroup

	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // burst all at once
			token := uuid.New()
			deadline := time.Now().Add(30 * time.Second)
			claimed, _, err := storage.TryClaim(ctx, db, j.ID, uuid.NewString(), token, deadline)
			if err != nil {
				return
			}
			if claimed != nil {
				claims.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if got := claims.Load(); got != 1 {
		t.Errorf("SKIP LOCKED: expected exactly 1 claim, got %d", got)
	}
}
