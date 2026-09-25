DROP INDEX IF EXISTS idx_jobs_tenant_in_flight;
ALTER TABLE tenants DROP COLUMN max_concurrency;
