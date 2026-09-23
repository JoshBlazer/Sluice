CREATE INDEX idx_jobs_dead ON jobs (id) WHERE state = 'dead';
