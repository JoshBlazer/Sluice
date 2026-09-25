package storage

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PruneFinishedJobs deletes up to limit succeeded or cancelled jobs that finished
// before cutoff, with their job_runs rows. Pending, scheduled, running and
// retrying jobs are never touched. Returns the number of jobs deleted.
// A pruned job's idempotency key becomes free again.
func PruneFinishedJobs(ctx context.Context, db *pgxpool.Pool, cutoff time.Time, limit int) (int64, error) {
	tag, err := db.Exec(ctx, `
		WITH doomed AS (
			SELECT id FROM jobs
			WHERE state IN ('succeeded', 'cancelled') AND completed_at < $1
			LIMIT $2
		), runs AS (
			DELETE FROM job_runs WHERE job_id IN (SELECT id FROM doomed)
		)
		DELETE FROM jobs WHERE id IN (SELECT id FROM doomed)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("prune finished jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// PruneDeadLetter deletes up to limit dead-letter entries moved before cutoff,
// along with their dead jobs and job_runs rows. Returns the number of entries deleted.
func PruneDeadLetter(ctx context.Context, db *pgxpool.Pool, cutoff time.Time, limit int) (int64, error) {
	tag, err := db.Exec(ctx, `
		WITH doomed AS (
			SELECT job_id FROM dead_letter WHERE moved_at < $1 LIMIT $2
		), runs AS (
			DELETE FROM job_runs WHERE job_id IN (SELECT job_id FROM doomed)
		), dead_jobs AS (
			DELETE FROM jobs WHERE id IN (SELECT job_id FROM doomed) AND state = 'dead'
		)
		DELETE FROM dead_letter WHERE job_id IN (SELECT job_id FROM doomed)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("prune dead letter: %w", err)
	}
	return tag.RowsAffected(), nil
}

var monthlyPartition = regexp.MustCompile(`^job_runs_(\d{4})_(\d{2})$`)

// DropJobRunsPartitionsBefore drops monthly job_runs partitions whose whole
// range ends at or before cutoff, which is far cheaper than deleting their rows.
// The default partition is never dropped. Returns the dropped partition names.
func DropJobRunsPartitionsBefore(ctx context.Context, db *pgxpool.Pool, cutoff time.Time) ([]string, error) {
	rows, err := db.Query(ctx, `
		SELECT c.relname
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		WHERE p.relname = 'job_runs'`)
	if err != nil {
		return nil, fmt.Errorf("list job_runs partitions: %w", err)
	}
	var expired []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan partition name: %w", err)
		}
		m := monthlyPartition.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		year, _ := strconv.Atoi(m[1])
		month, _ := strconv.Atoi(m[2])
		end := time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, time.UTC)
		if !end.After(cutoff) {
			expired = append(expired, name)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list job_runs partitions: %w", err)
	}

	var dropped []string
	for _, name := range expired {
		// name matched monthlyPartition, so it is safe to interpolate.
		if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
			return dropped, fmt.Errorf("drop partition %s: %w", name, err)
		}
		dropped = append(dropped, name)
	}
	return dropped, nil
}
