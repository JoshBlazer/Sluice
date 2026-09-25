package storage

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sluice/internal/job"
)

var ErrNotFound = errors.New("job not found")
var ErrClaimConflict = errors.New("job already claimed")
var ErrDuplicate = errors.New("duplicate idempotency key")

// NewPool connects to Postgres. Unless the DSN sets pool_max_conns explicitly, the
// pool may open at least minMaxConns connections (pgx's own default is only
// max(4, NumCPU), too few for a worker running many jobs at once).
func NewPool(ctx context.Context, dsn string, minMaxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	if !strings.Contains(dsn, "pool_max_conns") && cfg.MaxConns < minMaxConns {
		cfg.MaxConns = minMaxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// InsertJob persists a new job. Returns ErrDuplicate (wrapping the existing job ID)
// if an idempotency key conflict is detected — callers should fetch the original job.
func InsertJob(ctx context.Context, db *pgxpool.Pool, j *job.Job) error {
	_, err := db.Exec(ctx, `
		INSERT INTO jobs (
			id, tenant_id, type, payload, priority, state,
			run_at, attempt, max_retries, backoff_seconds,
			idempotency_key, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12
		)`,
		j.ID, j.TenantID, j.Type, []byte(j.Payload), j.Priority, string(j.State),
		j.RunAt, j.Attempt, j.MaxRetries, j.BackoffSeconds,
		j.IdempotencyKey, j.CreatedAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return ErrDuplicate
		}
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func GetJobByIdempotencyKey(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID, key string) (*job.Job, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

func GetJob(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) (*job.Job, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs WHERE id = $1`, id)

	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

const jobColumns = `id, tenant_id, type, payload, priority, state,
	run_at, claimed_at, claimed_by, claim_token, deadline,
	attempt, max_retries, backoff_seconds, idempotency_key,
	last_error, created_at, completed_at`

// TryClaim atomically claims a pending job for a worker and opens its job_runs row,
// in one statement. The row is locked with SELECT ... FOR UPDATE SKIP LOCKED, so
// concurrent claimers never both win. Returns (nil, uuid.Nil, nil) if the job is
// not pending (already claimed, cancelled, or finished). On success it returns the
// job as claimed and the run ID that later updates must be scoped to.
func TryClaim(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, workerID string, token uuid.UUID, deadline time.Time) (*job.Job, uuid.UUID, error) {
	runID := uuid.New()
	row := db.QueryRow(ctx, `
		WITH claimed AS (
			UPDATE jobs SET
				state       = 'running',
				claimed_at  = $2,
				claimed_by  = $3,
				claim_token = $4,
				deadline    = $5
			WHERE id = (
				SELECT id FROM jobs
				WHERE id = $1 AND state = 'pending'
				FOR UPDATE SKIP LOCKED)
			RETURNING `+jobColumns+`
		), run AS (
			INSERT INTO job_runs (id, job_id, tenant_id, attempt, started_at, state)
			SELECT $6, id, tenant_id, attempt, $2, 'running' FROM claimed
		)
		SELECT `+jobColumns+` FROM claimed`,
		jobID, time.Now(), workerID, token, deadline, runID)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, uuid.Nil, nil
	}
	if err != nil {
		return nil, uuid.Nil, fmt.Errorf("claim job %s: %w", jobID, err)
	}
	return j, runID, nil
}

// ExtendDeadline pushes the visibility deadline forward for a healthy running job.
// Called by the worker heartbeat loop so the stale-claim reaper doesn't reassign live jobs.
func ExtendDeadline(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, token uuid.UUID, deadline time.Time) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs SET deadline = $1
		WHERE id = $2 AND claim_token = $3 AND state IN ('claimed', 'running')`,
		deadline, jobID, token)
	if err != nil {
		return fmt.Errorf("extend deadline %s: %w", jobID, err)
	}
	return nil
}

// CompleteJob marks a job and its run succeeded, atomically. If the claim token
// doesn't match, it's a no-op: the job was reassigned and a stale worker must not
// overwrite the new owner's state.
// job_runs has no index on id; filtering on job_id as well keeps this an index scan.
func CompleteJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, runID uuid.UUID, token uuid.UUID) error {
	_, err := db.Exec(ctx, `
		WITH done AS (
			UPDATE jobs SET
				state        = 'succeeded',
				completed_at = $1,
				claim_token  = NULL,
				deadline     = NULL
			WHERE id = $2 AND claim_token = $3 AND state IN ('claimed', 'running')
			RETURNING id
		)
		UPDATE job_runs SET state = 'succeeded', finished_at = $1,
		    duration_ms = (EXTRACT(EPOCH FROM ($1 - started_at)) * 1000)::INT
		WHERE job_id = $2 AND id = $4 AND EXISTS (SELECT 1 FROM done)`,
		time.Now(), jobID, token, runID)
	if err != nil {
		return fmt.Errorf("complete job %s: %w", jobID, err)
	}
	return nil
}

// FailJob records a failed attempt on the job and its run, atomically. The job goes
// to 'failed' with run_at = nextRunAt (retried once the backoff elapses), or to
// 'dead' once attempts exceed max_retries. A stale claim token makes it a no-op.
func FailJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, runID uuid.UUID, token uuid.UUID, errMsg string, nextRunAt *time.Time) error {
	_, err := db.Exec(ctx, `
		WITH failed AS (
			UPDATE jobs SET
				state       = CASE WHEN attempt + 1 > max_retries THEN 'dead'::job_state ELSE 'failed'::job_state END,
				run_at      = CASE WHEN attempt + 1 > max_retries THEN run_at ELSE COALESCE($4, run_at) END,
				attempt     = attempt + 1,
				last_error  = $3,
				claim_token = NULL,
				deadline    = NULL
			WHERE id = $1 AND claim_token = $2 AND state IN ('claimed', 'running')
			RETURNING state
		)
		UPDATE job_runs SET state = failed.state, finished_at = $5, error = $3,
		    duration_ms = (EXTRACT(EPOCH FROM ($5 - started_at)) * 1000)::INT
		FROM failed
		WHERE job_runs.job_id = $1 AND job_runs.id = $6`,
		jobID, token, errMsg, nextRunAt, time.Now(), runID)
	if err != nil {
		return fmt.Errorf("fail job %s: %w", jobID, err)
	}
	return nil
}

// GetDueJobs returns up to limit scheduled jobs whose run_at is in the past.
func GetDueJobs(ctx context.Context, db *pgxpool.Pool, limit int) ([]*job.Job, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE state = 'scheduled' AND run_at <= NOW()
		ORDER BY priority, run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get due jobs: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// GetFailedReadyJobs returns failed jobs whose run_at is now due for retry.
func GetFailedReadyJobs(ctx context.Context, db *pgxpool.Pool, limit int) ([]*job.Job, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE state = 'failed' AND run_at <= NOW()
		ORDER BY priority, run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get failed ready jobs: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// GetStaleClaims returns claimed/running jobs whose deadline has passed.
func GetStaleClaims(ctx context.Context, db *pgxpool.Pool) ([]*job.Job, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE state IN ('claimed', 'running') AND deadline < NOW()`)
	if err != nil {
		return nil, fmt.Errorf("get stale claims: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// RequeueStaleJob handles a job whose deadline expired without a heartbeat extension.
// It closes the open job_run, then either moves the job back to pending (if retries remain)
// or to dead (if attempt+1 > max_retries). Returns dead=true when the job should not be
// re-enqueued because it has been routed to dead letter.
func RequeueStaleJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) (dead bool, err error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	now := time.Now()

	_, err = tx.Exec(ctx, `
		UPDATE job_runs SET state = 'failed', finished_at = $1, error = 'worker heartbeat expired',
		    duration_ms = (EXTRACT(EPOCH FROM ($1 - started_at)) * 1000)::INT
		WHERE job_id = $2 AND finished_at IS NULL`,
		now, jobID)
	if err != nil {
		return false, fmt.Errorf("close stale run %s: %w", jobID, err)
	}

	var newState string
	err = tx.QueryRow(ctx, `
		UPDATE jobs SET
			state       = CASE WHEN attempt + 1 > max_retries THEN 'dead'::job_state ELSE 'pending'::job_state END,
			attempt     = attempt + 1,
			claim_token = NULL,
			claimed_at  = NULL,
			claimed_by  = NULL,
			deadline    = NULL,
			last_error  = 'worker heartbeat expired'
		WHERE id = $1 AND state IN ('claimed', 'running') AND deadline < NOW()
		RETURNING state::text`, jobID).Scan(&newState)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				return false, fmt.Errorf("commit empty requeue: %w", commitErr)
			}
			return false, nil
		}
		return false, fmt.Errorf("requeue stale job %s: %w", jobID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit requeue: %w", err)
	}
	return newState == "dead", nil
}

// MoveToDeadLetter copies up to limit dead jobs that aren't yet in dead_letter into it.
// Selecting only unmoved jobs matters: a plain "newest N dead jobs" scan would keep
// re-reading already-moved rows and never reach older ones once N is exceeded.
func MoveToDeadLetter(ctx context.Context, db *pgxpool.Pool, limit int) (int64, error) {
	tag, err := db.Exec(ctx, `
		INSERT INTO dead_letter (job_id, tenant_id, final_error, attempt_count, original_job)
		SELECT j.id, j.tenant_id, j.last_error, j.attempt, to_jsonb(j.*)
		FROM jobs j
		WHERE j.state = 'dead'
		  AND NOT EXISTS (SELECT 1 FROM dead_letter d WHERE d.job_id = j.id)
		LIMIT $1
		ON CONFLICT (job_id) DO NOTHING`, limit)
	if err != nil {
		return 0, fmt.Errorf("insert dead_letter: %w", err)
	}
	return tag.RowsAffected(), nil
}

type ListFilter struct {
	TenantID *uuid.UUID
	State    *job.State
	// Retried limits results to jobs with at least one failed attempt.
	Retried bool
	Limit   int
	Offset  int
}

func ListJobs(ctx context.Context, db *pgxpool.Pool, f ListFilter) ([]*job.Job, error) {
	if f.Limit == 0 {
		f.Limit = 50
	}
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE ($1::uuid IS NULL OR tenant_id = $1)
		  AND ($2::job_state IS NULL OR state = $2)
		  AND (NOT $5 OR attempt > 0)
		ORDER BY created_at DESC
		LIMIT $3 OFFSET $4`,
		f.TenantID, statePtr(f.State), f.Limit, f.Offset, f.Retried)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// CancelJob marks a job that hasn't started (or is waiting out a retry backoff) as cancelled.
// A copy already sitting in Redis is harmless: TryClaim only claims pending jobs.
func CancelJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, tenantID uuid.UUID) error {
	tag, err := db.Exec(ctx, `
		UPDATE jobs SET state = 'cancelled', completed_at = NOW()
		WHERE id = $1 AND tenant_id = $2 AND state IN ('pending', 'scheduled', 'failed')`, jobID, tenantID)
	if err != nil {
		return fmt.Errorf("cancel job: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ReplayJob resets a dead job back to pending (attempt 0) so it will be re-executed,
// and removes its dead_letter entry so a second death is recorded afresh.
// Only jobs in 'dead' state belonging to the given tenant can be replayed.
func ReplayJob(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID, tenantID uuid.UUID) (*job.Job, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET
			state        = 'pending',
			attempt      = 0,
			run_at       = NOW(),
			last_error   = NULL,
			claim_token  = NULL,
			claimed_at   = NULL,
			claimed_by   = NULL,
			deadline     = NULL,
			completed_at = NULL
		WHERE id = $1 AND tenant_id = $2 AND state = 'dead'`,
		jobID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("replay job %s: %w", jobID, err)
	}
	if tag.RowsAffected() == 0 {
		return nil, ErrNotFound
	}
	if _, err := tx.Exec(ctx, `DELETE FROM dead_letter WHERE job_id = $1`, jobID); err != nil {
		return nil, fmt.Errorf("clear dead_letter %s: %w", jobID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit replay %s: %w", jobID, err)
	}
	return GetJob(ctx, db, jobID)
}

// GetJobForTenant fetches a job only if it belongs to the given tenant.
// Returns ErrNotFound if the job does not exist or belongs to a different tenant.
func GetJobForTenant(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, tenantID uuid.UUID) (*job.Job, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	j, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job for tenant: %w", err)
	}
	return j, nil
}

// GetPendingJobs returns pending jobs whose run_at is due, for the reconciliation loop.
// Re-enqueueing these is idempotent: TryClaim's SKIP LOCKED guards against double execution.
func GetPendingJobs(ctx context.Context, db *pgxpool.Pool, limit int) ([]*job.Job, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, type, payload, priority, state,
		       run_at, claimed_at, claimed_by, claim_token, deadline,
		       attempt, max_retries, backoff_seconds, idempotency_key,
		       last_error, created_at, completed_at
		FROM jobs
		WHERE state = 'pending' AND run_at <= NOW() - INTERVAL '1 minute'
		ORDER BY priority, run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending jobs: %w", err)
	}
	defer rows.Close()
	return collectJobs(rows)
}

// PromoteScheduledToPending moves a scheduled job to pending when its run_at has arrived.
func PromoteScheduledToPending(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs SET state = 'pending'
		WHERE id = $1 AND state = 'scheduled'`, jobID)
	if err != nil {
		return fmt.Errorf("promote scheduled job %s: %w", jobID, err)
	}
	return nil
}

// PromoteFailedToPending moves a failed job back to pending when its backoff has elapsed.
func PromoteFailedToPending(ctx context.Context, db *pgxpool.Pool, jobID uuid.UUID) error {
	_, err := db.Exec(ctx, `
		UPDATE jobs SET state = 'pending', run_at = NOW()
		WHERE id = $1 AND state = 'failed'`, jobID)
	if err != nil {
		return fmt.Errorf("promote failed job %s: %w", jobID, err)
	}
	return nil
}

func scanJob(row pgx.Row) (*job.Job, error) {
	var j job.Job
	var payload []byte
	var state string
	err := row.Scan(
		&j.ID, &j.TenantID, &j.Type, &payload, &j.Priority, &state,
		&j.RunAt, &j.ClaimedAt, &j.ClaimedBy, &j.ClaimToken, &j.Deadline,
		&j.Attempt, &j.MaxRetries, &j.BackoffSeconds, &j.IdempotencyKey,
		&j.LastError, &j.CreatedAt, &j.CompletedAt,
	)
	if err != nil {
		return nil, err
	}
	j.Payload = json.RawMessage(payload)
	j.State = job.State(state)
	return &j, nil
}

func collectJobs(rows pgx.Rows) ([]*job.Job, error) {
	var jobs []*job.Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job row: %w", err)
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

func statePtr(s *job.State) *string {
	if s == nil {
		return nil
	}
	v := string(*s)
	return &v
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

// EnsureJobRunsPartition creates the monthly job_runs child table for the given
// year/month if it doesn't already exist. Name and bounds are derived purely from
// time arithmetic, so the fmt.Sprintf into SQL is safe — no user input involved.
func EnsureJobRunsPartition(ctx context.Context, db *pgxpool.Pool, year int, month time.Month) error {
	name := fmt.Sprintf("job_runs_%04d_%02d", year, int(month))
	start := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
	_, err := db.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF job_runs FOR VALUES FROM ('%s') TO ('%s')`,
		name, start.Format("2006-01-02"), end.Format("2006-01-02"),
	))
	if err != nil {
		return fmt.Errorf("ensure partition %s: %w", name, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Schedule CRUD
// ---------------------------------------------------------------------------

type Schedule struct {
	ID          uuid.UUID       `json:"id"`
	TenantID    uuid.UUID       `json:"tenant_id"`
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Timezone    string          `json:"timezone"`
	JobTemplate json.RawMessage `json:"job_template"`
	Enabled     bool            `json:"enabled"`
	LastRunAt   *time.Time      `json:"last_run_at,omitempty"`
	NextRunAt   time.Time       `json:"next_run_at"`
}

func InsertSchedule(ctx context.Context, db *pgxpool.Pool, s *Schedule) error {
	_, err := db.Exec(ctx, `
		INSERT INTO schedules (id, tenant_id, name, cron, timezone, job_template, enabled, next_run_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		s.ID, s.TenantID, s.Name, s.Cron, s.Timezone, []byte(s.JobTemplate), s.Enabled, s.NextRunAt,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("schedule name already exists: %w", ErrDuplicate)
		}
		return fmt.Errorf("insert schedule: %w", err)
	}
	return nil
}

func GetSchedule(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, tenantID uuid.UUID) (*Schedule, error) {
	row := db.QueryRow(ctx, `
		SELECT id, tenant_id, name, cron, timezone, job_template, enabled, last_run_at, next_run_at
		FROM schedules WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	return scanSchedule(row)
}

func ListSchedules(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID) ([]*Schedule, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, name, cron, timezone, job_template, enabled, last_run_at, next_run_at
		FROM schedules WHERE tenant_id = $1 ORDER BY name`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list schedules: %w", err)
	}
	defer rows.Close()
	var out []*Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func DeleteSchedule(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, tenantID uuid.UUID) error {
	tag, err := db.Exec(ctx, `DELETE FROM schedules WHERE id = $1 AND tenant_id = $2`, id, tenantID)
	if err != nil {
		return fmt.Errorf("delete schedule: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func GetDueSchedules(ctx context.Context, db *pgxpool.Pool, limit int) ([]*Schedule, error) {
	rows, err := db.Query(ctx, `
		SELECT id, tenant_id, name, cron, timezone, job_template, enabled, last_run_at, next_run_at
		FROM schedules
		WHERE enabled = TRUE AND next_run_at <= NOW()
		ORDER BY next_run_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("get due schedules: %w", err)
	}
	defer rows.Close()
	var out []*Schedule
	for rows.Next() {
		s, err := scanSchedule(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func UpdateScheduleAfterRun(ctx context.Context, db *pgxpool.Pool, id uuid.UUID, lastRunAt, nextRunAt time.Time) error {
	_, err := db.Exec(ctx, `
		UPDATE schedules SET last_run_at = $1, next_run_at = $2
		WHERE id = $3`, lastRunAt, nextRunAt, id)
	return err
}

func scanSchedule(row pgx.Row) (*Schedule, error) {
	var s Schedule
	var tmpl []byte
	err := row.Scan(&s.ID, &s.TenantID, &s.Name, &s.Cron, &s.Timezone,
		&tmpl, &s.Enabled, &s.LastRunAt, &s.NextRunAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan schedule: %w", err)
	}
	s.JobTemplate = json.RawMessage(tmpl)
	return &s, nil
}

// ---------------------------------------------------------------------------
// Tenant lookup
// ---------------------------------------------------------------------------

type Tenant struct {
	ID        uuid.UUID
	Name      string
	RateLimit int
	Weight    int
	Status    string
	// WebhookSecret signs this tenant's webhook requests ("whsec_" + base64).
	WebhookSecret string
}

// NewWebhookSecret returns a fresh Standard Webhooks signing secret.
func NewWebhookSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate webhook secret: %w", err)
	}
	return "whsec_" + base64.StdEncoding.EncodeToString(b), nil
}

// RotateWebhookSecret replaces a tenant's webhook signing secret and returns it.
// Workers pick it up on their next tenant refresh (within 5 seconds, or on SIGHUP).
func RotateWebhookSecret(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID) (string, error) {
	secret, err := NewWebhookSecret()
	if err != nil {
		return "", err
	}
	tag, err := db.Exec(ctx, `UPDATE tenants SET webhook_secret = $1 WHERE id = $2`, secret, tenantID)
	if err != nil {
		return "", fmt.Errorf("rotate webhook secret: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	return secret, nil
}

// GetTenant fetches an active tenant by ID.
func GetTenant(ctx context.Context, db *pgxpool.Pool, id uuid.UUID) (*Tenant, error) {
	var t Tenant
	err := db.QueryRow(ctx, `
		SELECT id, name, rate_limit, weight, status, webhook_secret
		FROM tenants WHERE id = $1 AND status = 'active'`, id).
		Scan(&t.ID, &t.Name, &t.RateLimit, &t.Weight, &t.Status, &t.WebhookSecret)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get tenant %s: %w", id, err)
	}
	return &t, nil
}

// HashAPIKey returns the digest stored in tenants.api_key_hash for key.
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// NewAPIKey returns a fresh random API key. Only its hash is ever stored.
func NewAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate api key: %w", err)
	}
	return "sk_" + base64.RawURLEncoding.EncodeToString(b), nil
}

func GetTenantByAPIKey(ctx context.Context, db *pgxpool.Pool, apiKey string) (*Tenant, error) {
	var t Tenant
	err := db.QueryRow(ctx, `
		SELECT id, name, rate_limit, weight, status, webhook_secret
		FROM tenants WHERE api_key_hash = $1 AND status = 'active'`, HashAPIKey(apiKey)).
		Scan(&t.ID, &t.Name, &t.RateLimit, &t.Weight, &t.Status, &t.WebhookSecret)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return &t, nil
}

// InsertTenant creates an active tenant and returns its plaintext API key,
// which is not recoverable afterwards.
func InsertTenant(ctx context.Context, db *pgxpool.Pool, name string, rateLimit, weight int) (*Tenant, string, error) {
	key, err := NewAPIKey()
	if err != nil {
		return nil, "", err
	}
	secret, err := NewWebhookSecret()
	if err != nil {
		return nil, "", err
	}
	t := &Tenant{ID: uuid.New(), Name: name, RateLimit: rateLimit, Weight: weight, Status: "active", WebhookSecret: secret}
	_, err = db.Exec(ctx, `
		INSERT INTO tenants (id, name, api_key_hash, rate_limit, weight, status, webhook_secret)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		t.ID, t.Name, HashAPIKey(key), t.RateLimit, t.Weight, t.Status, t.WebhookSecret)
	if err != nil {
		return nil, "", fmt.Errorf("insert tenant: %w", err)
	}
	return t, key, nil
}

// RotateAPIKey replaces a tenant's API key and returns the new plaintext key.
// The old key stops working immediately.
func RotateAPIKey(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID) (string, error) {
	key, err := NewAPIKey()
	if err != nil {
		return "", err
	}
	tag, err := db.Exec(ctx, `UPDATE tenants SET api_key_hash = $1 WHERE id = $2`, HashAPIKey(key), tenantID)
	if err != nil {
		return "", fmt.Errorf("rotate api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", ErrNotFound
	}
	return key, nil
}

// ---------------------------------------------------------------------------
// Dashboard / stats queries
// ---------------------------------------------------------------------------

// CountJobsByState returns a map of state → count for one tenant.
func CountJobsByState(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID) (map[string]int64, error) {
	rows, err := db.Query(ctx, `SELECT state::text, COUNT(*) FROM jobs WHERE tenant_id = $1 GROUP BY state`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("count jobs by state: %w", err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		out[state] = count
	}
	return out, rows.Err()
}

// JobRun is a flattened view of job_runs joined with jobs for dashboard display.
type JobRun struct {
	RunID      uuid.UUID  `json:"run_id"`
	JobID      uuid.UUID  `json:"job_id"`
	TenantID   uuid.UUID  `json:"tenant_id"`
	JobType    string     `json:"type"`
	Attempt    int        `json:"attempt"`
	State      string     `json:"state"`
	DurationMs *int       `json:"duration_ms"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Error      *string    `json:"error,omitempty"`
}

func ListRecentRuns(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID, limit int) ([]*JobRun, error) {
	rows, err := db.Query(ctx, `
		SELECT jr.id, jr.job_id, jr.tenant_id, j.type, jr.attempt,
		       jr.state::text, jr.duration_ms, jr.started_at, jr.finished_at, jr.error
		FROM job_runs jr
		JOIN jobs j ON j.id = jr.job_id
		WHERE jr.tenant_id = $1
		ORDER BY jr.started_at DESC
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent runs: %w", err)
	}
	return collectRuns(rows)
}

// ListJobRuns returns every execution attempt of one job, oldest first. It returns
// an empty list, not an error, for a job the tenant doesn't own.
func ListJobRuns(ctx context.Context, db *pgxpool.Pool, jobID, tenantID uuid.UUID) ([]*JobRun, error) {
	rows, err := db.Query(ctx, `
		SELECT jr.id, jr.job_id, jr.tenant_id, j.type, jr.attempt,
		       jr.state::text, jr.duration_ms, jr.started_at, jr.finished_at, jr.error
		FROM job_runs jr
		JOIN jobs j ON j.id = jr.job_id
		WHERE jr.job_id = $1 AND jr.tenant_id = $2
		ORDER BY jr.started_at`, jobID, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list job runs %s: %w", jobID, err)
	}
	return collectRuns(rows)
}

func collectRuns(rows pgx.Rows) ([]*JobRun, error) {
	defer rows.Close()
	out := []*JobRun{}
	for rows.Next() {
		var r JobRun
		if err := rows.Scan(&r.RunID, &r.JobID, &r.TenantID, &r.JobType, &r.Attempt,
			&r.State, &r.DurationMs, &r.StartedAt, &r.FinishedAt, &r.Error); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		out = append(out, &r)
	}
	return out, rows.Err()
}

// DeadLetterEntry is a single dead-letter record for dashboard display.
type DeadLetterEntry struct {
	JobID        uuid.UUID `json:"job_id"`
	TenantID     uuid.UUID `json:"tenant_id"`
	AttemptCount int       `json:"attempt_count"`
	FinalError   *string   `json:"final_error,omitempty"`
	MovedAt      time.Time `json:"moved_at"`
}

func ListDeadLetter(ctx context.Context, db *pgxpool.Pool, tenantID uuid.UUID, limit int) ([]*DeadLetterEntry, error) {
	rows, err := db.Query(ctx, `
		SELECT job_id, tenant_id, attempt_count, final_error, moved_at
		FROM dead_letter
		WHERE tenant_id = $1
		ORDER BY moved_at DESC
		LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letter: %w", err)
	}
	defer rows.Close()
	var out []*DeadLetterEntry
	for rows.Next() {
		var e DeadLetterEntry
		if err := rows.Scan(&e.JobID, &e.TenantID, &e.AttemptCount, &e.FinalError, &e.MovedAt); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}

// GetTenants returns all active tenants with their scheduling weights.
// Used by workers to build the weighted-fair-queue dequeue set.
func GetTenants(ctx context.Context, db *pgxpool.Pool) ([]*Tenant, error) {
	rows, err := db.Query(ctx, `
		SELECT id, name, rate_limit, weight, status, webhook_secret
		FROM tenants WHERE status = 'active'`)
	if err != nil {
		return nil, fmt.Errorf("get tenants: %w", err)
	}
	defer rows.Close()
	var out []*Tenant
	for rows.Next() {
		var t Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.RateLimit, &t.Weight, &t.Status, &t.WebhookSecret); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		out = append(out, &t)
	}
	return out, rows.Err()
}
