-- Postgres cannot drop a value from an enum. 'cancelled' stays defined but
-- unused; cancelled jobs are removed so older code never sees the state.
DELETE FROM job_runs WHERE job_id IN (SELECT id FROM jobs WHERE state::text = 'cancelled');
DELETE FROM jobs WHERE state::text = 'cancelled';
