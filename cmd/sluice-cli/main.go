package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/sluice/internal/job"
	"github.com/sluice/internal/queue"
	"github.com/sluice/internal/storage"
)

func main() {
	postgresURL := flag.String("postgres-url", env("SLUICE_POSTGRES_URL", "postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable"), "postgres DSN")
	redisAddr := flag.String("redis-addr", env("SLUICE_REDIS_ADDR", "localhost:6379"), "redis address")
	flag.Parse()

	if flag.NArg() == 0 {
		usage()
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := pgxpool.New(ctx, *postgresURL)
	if err != nil {
		fatalf("connect postgres: %v", err)
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	defer rdb.Close()

	q := queue.New(rdb)

	switch flag.Arg(0) {
	case "replay":
		cmdReplay(ctx, db, q, flag.Args()[1:])
	case "force-fail":
		cmdForceFail(ctx, db, flag.Args()[1:])
	case "drain":
		cmdDrain(ctx, rdb)
	case "dump-scheduler":
		cmdDumpScheduler(ctx, db, rdb)
	case "list-dead":
		cmdListDead(ctx, db)
	case "queue-depth":
		cmdQueueDepth(ctx, db, q)
	case "create-tenant":
		cmdCreateTenant(ctx, db, flag.Args()[1:])
	case "rotate-key":
		cmdRotateKey(ctx, db, flag.Args()[1:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", flag.Arg(0))
		usage()
		os.Exit(1)
	}
}

func cmdReplay(ctx context.Context, db *pgxpool.Pool, q *queue.Queue, args []string) {
	if len(args) == 0 {
		fatalf("usage: sluice-cli replay <job-id>")
	}
	jobID, err := uuid.Parse(args[0])
	if err != nil {
		fatalf("invalid job id: %v", err)
	}

	j, err := storage.GetJob(ctx, db, jobID)
	if err != nil {
		fatalf("get job: %v", err)
	}
	if j.State != job.StateDead {
		fatalf("job %s is in state %s, not dead — only dead jobs can be replayed", jobID, j.State)
	}

	j, err = storage.ReplayJob(ctx, db, jobID, j.TenantID)
	if err != nil {
		fatalf("replay: %v", err)
	}
	if err := q.Enqueue(ctx, j.TenantID, jobID, j.Priority); err != nil {
		fatalf("enqueue: %v", err)
	}
	fmt.Printf("replayed job %s\n", jobID)
}

func cmdForceFail(ctx context.Context, db *pgxpool.Pool, args []string) {
	if len(args) == 0 {
		fatalf("usage: sluice-cli force-fail <job-id>")
	}
	jobID, err := uuid.Parse(args[0])
	if err != nil {
		fatalf("invalid job id: %v", err)
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, `
		UPDATE jobs SET state = 'dead', last_error = 'force-failed by operator',
		    claim_token = NULL, deadline = NULL
		WHERE id = $1 AND state IN ('claimed', 'running', 'pending', 'scheduled', 'failed')`, jobID)
	if err != nil {
		fatalf("force-fail: %v", err)
	}
	if tag.RowsAffected() == 0 {
		fatalf("job %s not found or already terminal", jobID)
	}
	// Close any run a worker still has open; its eventual result is discarded
	// because the claim token was cleared above.
	if _, err := tx.Exec(ctx, `
		UPDATE job_runs SET state = 'dead', finished_at = NOW(), error = 'force-failed by operator',
		    duration_ms = (EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000)::INT
		WHERE job_id = $1 AND finished_at IS NULL`, jobID); err != nil {
		fatalf("close open run: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		fatalf("commit: %v", err)
	}
	fmt.Printf("force-failed job %s\n", jobID)
}

func cmdListDead(ctx context.Context, db *pgxpool.Pool) {
	rows, err := db.Query(ctx, `
		SELECT job_id, tenant_id, moved_at, final_error, attempt_count
		FROM dead_letter
		ORDER BY moved_at DESC
		LIMIT 50`)
	if err != nil {
		fatalf("query dead_letter: %v", err)
	}
	defer rows.Close()

	fmt.Printf("%-36s  %-36s  %-25s  %s\n", "JOB ID", "TENANT", "MOVED AT", "ERROR")
	fmt.Println(repeat("-", 130))
	count := 0
	for rows.Next() {
		var jobID, tenantID uuid.UUID
		var movedAt time.Time
		var finalError *string
		var attempts int
		if err := rows.Scan(&jobID, &tenantID, &movedAt, &finalError, &attempts); err != nil {
			fatalf("scan: %v", err)
		}
		errStr := ""
		if finalError != nil {
			errStr = *finalError
			if len(errStr) > 50 {
				errStr = errStr[:50] + "…"
			}
		}
		fmt.Printf("%-36s  %-36s  %-25s  %s\n", jobID, tenantID, movedAt.Format(time.RFC3339), errStr)
		count++
	}
	if count == 0 {
		fmt.Println("(no dead-letter jobs)")
	}
}

func cmdQueueDepth(ctx context.Context, db *pgxpool.Pool, q *queue.Queue) {
	tenants, err := storage.GetTenants(ctx, db)
	if err != nil {
		fatalf("load tenants: %v", err)
	}
	ids := make([]uuid.UUID, len(tenants))
	names := make(map[uuid.UUID]string, len(tenants))
	for i, t := range tenants {
		ids[i] = t.ID
		names[t.ID] = t.Name
	}
	depths, err := q.Depths(ctx, ids)
	if err != nil {
		fatalf("read queue depths: %v", err)
	}

	totals := map[string]int64{}
	fmt.Printf("%-24s  %-8s  %s\n", "TENANT", "PRIORITY", "DEPTH")
	fmt.Println(repeat("-", 44))
	for _, d := range depths {
		totals[d.Priority] += d.Depth
		if d.Depth > 0 {
			fmt.Printf("%-24s  %-8s  %d\n", names[d.TenantID], d.Priority, d.Depth)
		}
	}
	fmt.Println(repeat("-", 44))
	for _, p := range []string{"high", "normal", "low"} {
		fmt.Printf("%-24s  %-8s  %d\n", "(all tenants)", p, totals[p])
	}
}

func cmdCreateTenant(ctx context.Context, db *pgxpool.Pool, args []string) {
	fs := flag.NewFlagSet("create-tenant", flag.ExitOnError)
	rateLimit := fs.Int("rate-limit", 100, "max job submissions per second (0 = unlimited)")
	weight := fs.Int("weight", 100, "fair-queuing weight relative to other tenants")
	fs.Parse(args) //nolint:errcheck // ExitOnError exits instead of returning
	if fs.NArg() != 1 {
		fatalf("usage: sluice-cli create-tenant [-rate-limit N] [-weight N] <name>")
	}
	if *weight <= 0 {
		fatalf("weight must be positive")
	}

	t, key, err := storage.InsertTenant(ctx, db, fs.Arg(0), *rateLimit, *weight)
	if err != nil {
		fatalf("create tenant: %v", err)
	}
	fmt.Printf("tenant id: %s\napi key:   %s\n\nStore the key now — only its hash is kept.\n", t.ID, key)
}

func cmdRotateKey(ctx context.Context, db *pgxpool.Pool, args []string) {
	if len(args) != 1 {
		fatalf("usage: sluice-cli rotate-key <tenant-id>")
	}
	tenantID, err := uuid.Parse(args[0])
	if err != nil {
		fatalf("invalid tenant id: %v", err)
	}
	key, err := storage.RotateAPIKey(ctx, db, tenantID)
	if err != nil {
		fatalf("rotate key: %v", err)
	}
	fmt.Printf("new api key: %s\n\nThe old key no longer works. Store this one now — only its hash is kept.\n", key)
}

func cmdDrain(ctx context.Context, rdb *redis.Client) {
	var cursor uint64
	var totalJobs int64
	var queueCount int

	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:*", 100).Result()
		if err != nil {
			fatalf("scan redis: %v", err)
		}
		for _, key := range keys {
			n, err := rdb.LLen(ctx, key).Result()
			if err != nil || n == 0 {
				continue
			}
			if err := rdb.Del(ctx, key).Err(); err != nil {
				fmt.Fprintf(os.Stderr, "warn: del %s: %v\n", key, err)
				continue
			}
			totalJobs += n
			queueCount++
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	// Clear "already queued" markers too, or the reconciler couldn't re-enqueue the
	// drained jobs until the markers expired.
	cursor = 0
	for {
		keys, next, err := rdb.Scan(ctx, cursor, queue.EnqueuedPattern, 500).Result()
		if err != nil {
			fatalf("scan redis: %v", err)
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				fmt.Fprintf(os.Stderr, "warn: del enqueued markers: %v\n", err)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	if totalJobs == 0 {
		fmt.Println("queues already empty")
		return
	}
	fmt.Printf("removed %d jobs from %d queues\n", totalJobs, queueCount)
	fmt.Println("jobs are preserved in postgres and will be re-enqueued by the reconciler when ready")
}

func cmdDumpScheduler(ctx context.Context, db *pgxpool.Pool, rdb *redis.Client) {
	fmt.Println("JOB COUNTS")
	rows, err := db.Query(ctx, `SELECT state::text, COUNT(*) FROM jobs GROUP BY state ORDER BY state`)
	if err != nil {
		fatalf("query job counts: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			fatalf("scan job counts: %v", err)
		}
		fmt.Printf("  %-12s %d\n", state, count)
	}

	fmt.Println("\nACTIVE WORKERS")
	rows2, err := db.Query(ctx, `
		SELECT id, claimed_by, state::text, claimed_at
		FROM jobs WHERE state IN ('claimed', 'running')
		ORDER BY claimed_at`)
	if err != nil {
		fatalf("query active workers: %v", err)
	}
	defer rows2.Close()
	activeCount := 0
	for rows2.Next() {
		var jobID uuid.UUID
		var claimedBy *string
		var state string
		var claimedAt *time.Time
		if err := rows2.Scan(&jobID, &claimedBy, &state, &claimedAt); err != nil {
			fatalf("scan active worker: %v", err)
		}
		worker := "(unknown)"
		if claimedBy != nil {
			worker = *claimedBy
		}
		elapsed := ""
		if claimedAt != nil {
			elapsed = fmt.Sprintf("(%s ago)", time.Since(*claimedAt).Round(time.Second))
		}
		fmt.Printf("  %-10s  %-36s  worker %.8s…  %s\n", state, jobID, worker, elapsed)
		activeCount++
	}
	if activeCount == 0 {
		fmt.Println("  (none)")
	}

	fmt.Println("\nUPCOMING SCHEDULES")
	rows3, err := db.Query(ctx, `
		SELECT name, cron, next_run_at FROM schedules
		WHERE enabled = TRUE ORDER BY next_run_at LIMIT 10`)
	if err != nil {
		fatalf("query schedules: %v", err)
	}
	defer rows3.Close()
	schedCount := 0
	for rows3.Next() {
		var name, cron string
		var nextRun time.Time
		if err := rows3.Scan(&name, &cron, &nextRun); err != nil {
			fatalf("scan schedule: %v", err)
		}
		in := time.Until(nextRun).Round(time.Second)
		fmt.Printf("  %-24s  %-20s  %s  (in %s)\n", name, cron, nextRun.UTC().Format(time.RFC3339), in)
		schedCount++
	}
	if schedCount == 0 {
		fmt.Println("  (no enabled schedules)")
	}

	fmt.Println("\nREDIS QUEUE DEPTHS")
	var cursor uint64
	type qd struct {
		key string
		n   int64
	}
	var depths []qd
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "queue:*", 100).Result()
		if err != nil {
			break
		}
		for _, key := range keys {
			n, err := rdb.LLen(ctx, key).Result()
			if err != nil || n == 0 {
				continue
			}
			depths = append(depths, qd{key, n})
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(depths) == 0 {
		fmt.Println("  (all queues empty)")
	}
	for _, d := range depths {
		fmt.Printf("  %-44s  %d\n", d.key, d.n)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `sluice-cli — Sluice admin tool

Commands:
  replay <job-id>     re-enqueue a dead-letter job from the beginning
  force-fail <job-id> mark a job dead immediately
  drain               flush all Redis queues (jobs stay in postgres)
  dump-scheduler      show job counts, active workers, schedules, queue depths
  list-dead           list dead-letter jobs (most recent 50)
  queue-depth         show pending job counts per tenant and priority
  create-tenant       create a tenant and print its API key
                      (flags: -rate-limit N, -weight N)
  rotate-key <id>     issue a new API key for a tenant, revoking the old one

Flags:`)
	flag.PrintDefaults()
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}
