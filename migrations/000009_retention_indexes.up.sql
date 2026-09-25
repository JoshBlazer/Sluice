-- Support the scheduler's retention pass: find finished jobs and old
-- dead-letter entries by age without scanning the whole table.
CREATE INDEX idx_jobs_finished ON jobs (completed_at) WHERE state IN ('succeeded', 'cancelled');
CREATE INDEX idx_dead_letter_moved ON dead_letter (moved_at);
