//go:build integration

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/storage"
	"github.com/sluice/internal/testutil"
)

func jobExists(t *testing.T, id uuid.UUID) bool {
	t.Helper()
	_, err := storage.GetJob(context.Background(), testutil.DB(t), id)
	return err == nil
}

func TestPruneFinishedJobs(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)
	old := time.Now().Add(-40 * 24 * time.Hour)
	recent := time.Now().Add(-time.Hour)

	finished := func(state job.State, at time.Time) *job.Job {
		j := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.State = state })
		if _, err := db.Exec(ctx, `UPDATE jobs SET completed_at = $1 WHERE id = $2`, at, j.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(ctx, `INSERT INTO job_runs (id, job_id, tenant_id, attempt, started_at, state)
			VALUES ($1, $2, $3, 0, $4, $5::job_state)`, uuid.New(), j.ID, tn.ID, at, string(state)); err != nil {
			t.Fatal(err)
		}
		return j
	}
	oldSucceeded := finished(job.StateSucceeded, old)
	oldCancelled := finished(job.StateCancelled, old)
	recentSucceeded := finished(job.StateSucceeded, recent)
	// Old but unfinished: must survive whatever its age.
	oldRetrying := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) {
		j.State = job.StateFailed
		j.CreatedAt = old
	})
	oldPending := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.CreatedAt = old })

	cutoff := time.Now().Add(-30 * 24 * time.Hour)
	for {
		n, err := storage.PruneFinishedJobs(ctx, db, cutoff, 1)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
	}

	for _, j := range []*job.Job{oldSucceeded, oldCancelled} {
		if jobExists(t, j.ID) {
			t.Errorf("old %s job was not pruned", j.State)
		}
	}
	for _, j := range []*job.Job{recentSucceeded, oldRetrying, oldPending} {
		if !jobExists(t, j.ID) {
			t.Errorf("%s job created %v was pruned but must be kept", j.State, j.CreatedAt)
		}
	}
	var orphanRuns int
	db.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE job_id = ANY($1)`,
		[]uuid.UUID{oldSucceeded.ID, oldCancelled.ID}).Scan(&orphanRuns)
	if orphanRuns != 0 {
		t.Fatalf("%d job_runs rows left behind for pruned jobs", orphanRuns)
	}
}

func TestPruneDeadLetter(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)
	tn, _ := testutil.Tenant(t, db, 0, 100)

	oldDead := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.State = job.StateDead })
	newDead := testutil.InsertJob(t, db, tn.ID, "https://example.com", func(j *job.Job) { j.State = job.StateDead })
	if _, err := storage.MoveToDeadLetter(ctx, db, 1000); err != nil {
		t.Fatal(err)
	}
	db.Exec(ctx, `UPDATE dead_letter SET moved_at = NOW() - INTERVAL '40 days' WHERE job_id = $1`, oldDead.ID)

	if _, err := storage.PruneDeadLetter(ctx, db, time.Now().Add(-30*24*time.Hour), 1000); err != nil {
		t.Fatal(err)
	}
	entries, _ := storage.ListDeadLetter(ctx, db, tn.ID, 10)
	if len(entries) != 1 || entries[0].JobID != newDead.ID {
		t.Fatalf("dead_letter = %v, want only the recent entry", entries)
	}
	if jobExists(t, oldDead.ID) {
		t.Fatal("old dead job row was not pruned")
	}
}

func TestDropJobRunsPartitionsBefore(t *testing.T) {
	ctx := context.Background()
	db := testutil.DB(t)

	// A far-past month no real data lives in, plus the current month's partition.
	if err := storage.EnsureJobRunsPartition(ctx, db, 2001, time.January); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := storage.EnsureJobRunsPartition(ctx, db, now.Year(), now.Month()); err != nil {
		t.Fatal(err)
	}

	dropped, err := storage.DropJobRunsPartitionsBefore(ctx, db, time.Date(2001, time.March, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(dropped) != 1 || dropped[0] != "job_runs_2001_01" {
		t.Fatalf("dropped %v, want only job_runs_2001_01", dropped)
	}

	var n int
	db.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname = $1`,
		now.Format("job_runs_2006_01")).Scan(&n)
	if n != 1 {
		t.Fatal("current month's partition was dropped")
	}
	db.QueryRow(ctx, `SELECT count(*) FROM pg_class WHERE relname = 'job_runs_default'`).Scan(&n)
	if n != 1 {
		t.Fatal("default partition was dropped")
	}
}
