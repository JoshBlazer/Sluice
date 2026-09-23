-- Cancelled jobs are kept rather than deleted, so their job_runs history isn't
-- orphaned and their idempotency key can't be reused to resubmit the job.
ALTER TYPE job_state ADD VALUE IF NOT EXISTS 'cancelled';
