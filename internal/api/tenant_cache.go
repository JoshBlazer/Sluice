package api

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sluice/internal/storage"
)

// tenantCacheTTL is how long an API key's tenant is reused before re-reading
// Postgres, and so how long a rotated or disabled key keeps working.
const tenantCacheTTL = 10 * time.Second

// tenantCache saves a Postgres round trip on every authenticated request.
// Only successful lookups are cached, so guessing keys can't grow it; it holds
// at most one entry per valid key.
type tenantCache struct {
	db *pgxpool.Pool

	mu      sync.Mutex
	entries map[string]tenantCacheEntry
}

type tenantCacheEntry struct {
	tenant  *storage.Tenant
	expires time.Time
}

func newTenantCache(db *pgxpool.Pool) *tenantCache {
	return &tenantCache{db: db, entries: map[string]tenantCacheEntry{}}
}

func (c *tenantCache) lookup(ctx context.Context, apiKey string) (*storage.Tenant, error) {
	hash := storage.HashAPIKey(apiKey)
	now := time.Now()

	c.mu.Lock()
	e, ok := c.entries[hash]
	c.mu.Unlock()
	if ok && now.Before(e.expires) {
		return e.tenant, nil
	}

	t, err := storage.GetTenantByAPIKey(ctx, c.db, apiKey)
	if err != nil {
		if ok {
			c.mu.Lock()
			delete(c.entries, hash)
			c.mu.Unlock()
		}
		return nil, err
	}
	c.mu.Lock()
	c.entries[hash] = tenantCacheEntry{tenant: t, expires: now.Add(tenantCacheTTL)}
	c.mu.Unlock()
	return t, nil
}
