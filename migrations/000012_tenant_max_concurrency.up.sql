-- Cap on how many of a tenant's jobs run at once. 0 means unlimited.
ALTER TABLE tenants ADD COLUMN max_concurrency INT NOT NULL DEFAULT 0 CHECK (max_concurrency >= 0);
-- Counting a tenant's in-flight jobs happens on every claim for limited tenants.
CREATE INDEX idx_jobs_tenant_in_flight ON jobs (tenant_id) WHERE state IN ('claimed', 'running');
