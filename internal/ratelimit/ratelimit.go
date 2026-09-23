package ratelimit

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Limiter is a per-tenant token bucket stored in Redis at ratelimit:{tenant_id}.
// The bucket holds up to limit tokens and refills at limit tokens per second,
// so a tenant can burst one second's worth of submissions.
type Limiter struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Limiter {
	return &Limiter{rdb: rdb}
}

// Refill and take run atomically in Redis, timed by Redis's own clock so API
// replicas with skewed clocks share one consistent bucket.
var takeScript = redis.NewScript(`
local rate = tonumber(ARGV[1])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1]) or rate
local ts = tonumber(state[2]) or now
tokens = math.min(rate, tokens + (now - ts) * rate / 1000)
local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], 2000)
return allowed
`)

// Allow takes one token from tenantID's bucket, where limit is jobs/sec.
// A limit of zero or less means the tenant is unlimited.
func (l *Limiter) Allow(ctx context.Context, tenantID uuid.UUID, limit int) (bool, error) {
	if limit <= 0 {
		return true, nil
	}
	allowed, err := takeScript.Run(ctx, l.rdb, []string{"ratelimit:" + tenantID.String()}, limit).Int()
	if err != nil {
		return false, fmt.Errorf("ratelimit: %w", err)
	}
	return allowed == 1, nil
}
