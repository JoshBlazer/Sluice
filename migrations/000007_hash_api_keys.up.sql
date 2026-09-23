-- Store only a SHA-256 digest of each API key. Keys are high-entropy random
-- tokens, so an unsalted fast hash is sufficient and keeps lookup indexable.
ALTER TABLE tenants ADD COLUMN api_key_hash TEXT;
UPDATE tenants SET api_key_hash = encode(sha256(convert_to(api_key, 'UTF8')), 'hex');
ALTER TABLE tenants ALTER COLUMN api_key_hash SET NOT NULL;
ALTER TABLE tenants ADD CONSTRAINT tenants_api_key_hash_key UNIQUE (api_key_hash);
ALTER TABLE tenants DROP COLUMN api_key;
