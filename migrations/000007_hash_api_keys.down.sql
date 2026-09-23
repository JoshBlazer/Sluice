-- Plaintext keys cannot be recovered from their digests. Every tenant except
-- the dev tenant must be issued a new key after rolling back.
ALTER TABLE tenants ADD COLUMN api_key TEXT;
UPDATE tenants SET api_key = 'reissue-' || id::text;
UPDATE tenants SET api_key = 'dev-token' WHERE id = '00000000-0000-0000-0000-000000000001';
ALTER TABLE tenants ALTER COLUMN api_key SET NOT NULL;
ALTER TABLE tenants ADD CONSTRAINT tenants_api_key_key UNIQUE (api_key);
ALTER TABLE tenants DROP COLUMN api_key_hash;
