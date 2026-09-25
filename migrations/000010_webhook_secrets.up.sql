-- Per-tenant secret for signing webhook requests (Standard Webhooks format:
-- "whsec_" + base64 of 32 random bytes). Unlike API keys it must be readable,
-- because workers sign with it.
ALTER TABLE tenants ADD COLUMN webhook_secret TEXT;
UPDATE tenants SET webhook_secret = 'whsec_' || encode(decode(
    replace(gen_random_uuid()::text, '-', '') || replace(gen_random_uuid()::text, '-', ''), 'hex'), 'base64');
ALTER TABLE tenants ALTER COLUMN webhook_secret SET NOT NULL;
