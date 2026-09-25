UPDATE tenants SET status = 'active'
WHERE id = '00000000-0000-0000-0000-000000000001'
  AND api_key_hash = encode(sha256(convert_to('dev-token', 'UTF8')), 'hex');
