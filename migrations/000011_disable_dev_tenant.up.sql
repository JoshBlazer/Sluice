-- Migration 2 seeds a "dev" tenant whose API key, "dev-token", is public. Disable
-- it everywhere so no deployment accepts that key by default. For local work,
-- `sluice-cli enable-dev-tenant` turns it back on.
UPDATE tenants SET status = 'disabled'
WHERE id = '00000000-0000-0000-0000-000000000001'
  AND api_key_hash = encode(sha256(convert_to('dev-token', 'UTF8')), 'hex');
