//go:build integration

// Package testutil connects integration tests to the docker-compose Postgres
// and Redis. Tests skip when the stack is down, unless SLUICE_TEST_REQUIRE_INFRA
// is set (as in CI), where a missing stack is a failure.
package testutil

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/storage"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// redisTestDB keeps test keys out of the dev queues in DB 0.
const redisTestDB = 15

func unavailable(t testing.TB, what string, err error) {
	t.Helper()
	if os.Getenv("SLUICE_TEST_REQUIRE_INFRA") != "" {
		t.Fatalf("%s not available: %v", what, err)
	}
	t.Skipf("%s not available (start it with `make up && make migrate-up`): %v", what, err)
}

func DB(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("SLUICE_TEST_POSTGRES_URL")
	if dsn == "" {
		dsn = "postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err == nil {
		err = db.Ping(ctx)
	}
	if err != nil {
		unavailable(t, "postgres", err)
	}
	t.Cleanup(db.Close)
	return db
}

// Redis returns a client on a dedicated, freshly flushed test database.
func Redis(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("SLUICE_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, DB: redisTestDB})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		unavailable(t, "redis", err)
	}
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush redis test db: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}

// Tenant creates a throwaway tenant and deletes it, with everything it owns,
// when the test ends.
func Tenant(t testing.TB, db *pgxpool.Pool, rateLimit, weight int) (*storage.Tenant, string) {
	t.Helper()
	ctx := context.Background()
	tn, key, err := storage.InsertTenant(ctx, db, "test-"+t.Name(), rateLimit, weight, 0)
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	t.Cleanup(func() {
		for _, q := range []string{
			`DELETE FROM job_runs WHERE tenant_id = $1`,
			`DELETE FROM dead_letter WHERE tenant_id = $1`,
			`DELETE FROM jobs WHERE tenant_id = $1`,
			`DELETE FROM schedules WHERE tenant_id = $1`,
			`DELETE FROM tenants WHERE id = $1`,
		} {
			if _, err := db.Exec(ctx, q, tn.ID); err != nil {
				t.Errorf("cleanup tenant %s: %v", tn.ID, err)
			}
		}
	})
	return tn, key
}

// InsertJob stores a pending webhook job for tenantID, applying mutate first.
func InsertJob(t testing.TB, db *pgxpool.Pool, tenantID uuid.UUID, url string, mutate func(*job.Job)) *job.Job {
	t.Helper()
	now := time.Now()
	j := &job.Job{
		ID:             uuid.New(),
		TenantID:       tenantID,
		Type:           "webhook",
		Payload:        []byte(`{"url":"` + url + `"}`),
		Priority:       job.PriorityNormal,
		State:          job.StatePending,
		RunAt:          now,
		MaxRetries:     job.DefaultMaxRetries,
		BackoffSeconds: 1,
		CreatedAt:      now,
	}
	if mutate != nil {
		mutate(j)
	}
	if err := storage.InsertJob(context.Background(), db, j); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	return j
}

// Eventually polls cond until it returns true or timeout elapses.
func Eventually(t testing.TB, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, msg)
}

// EtcdEndpoints returns the test etcd endpoints after checking one is reachable.
func EtcdEndpoints(t testing.TB) []string {
	t.Helper()
	ep := os.Getenv("SLUICE_TEST_ETCD_ENDPOINTS")
	if ep == "" {
		ep = "localhost:2379"
	}
	endpoints := strings.Split(ep, ",")
	c, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 3 * time.Second})
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err = c.Status(ctx, endpoints[0])
		cancel()
		c.Close()
	}
	if err != nil {
		unavailable(t, "etcd", err)
	}
	return endpoints
}
