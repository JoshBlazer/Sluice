# Sluice — Build Instructions for Claude

This file governs how Claude approaches building this project. Read it at the start of every session. Follow it even when a deviation seems reasonable in the moment.

---

## What We're Building

**Sluice** is a horizontally scalable, durable job scheduler in Go. The full design is in `README.md` and `architecture.md`. Read both before touching code.

The short version: Postgres is the source of truth, Redis is the hot queue, and a single Go binary runs as `api`, `scheduler`, or `worker` based on a `--role` flag.

---

## The Four Build Phases

Work strictly in phase order. Do not start Phase 2 until Phase 1 is complete and manually verified. Do not let features from later phases bleed into earlier ones.

---

### Phase 1 — Core Engine (start here)

**Goal:** jobs can be submitted, queued, executed, and retried. No HA, no multi-tenancy, no dashboard.

Deliverables, in order:

1. **Project scaffold**
   - `go mod init` with module path `github.com/sluice`
   - Directory structure per `README.md`: `cmd/`, `internal/`, `migrations/`, `proto/`, `deploy/`, `web/`
   - `docker-compose.yml` with Postgres 16, Redis 7. No etcd yet.
   - `Makefile` with targets: `dev`, `test`, `lint`, `migrate-up`, `docker-build`

2. **Database migrations** (`migrations/` via golang-migrate)
   - `jobs` table with all columns from `architecture.md` including `job_state` enum
   - `schedules` table
   - `job_runs` table (no partitioning yet, add it in Phase 2)
   - `dead_letter` table
   - All indexes from the architecture doc

3. **Domain types** (`internal/job/`)
   - `Job` struct, `JobState` enum, state machine transitions as pure functions
   - No I/O in this package. Just types and logic.

4. **Storage layer** (`internal/storage/`)
   - Use `pgx/v5` directly. No ORM.
   - Functions needed: `InsertJob`, `GetJob`, `TryClaim`, `CompleteJob`, `FailJob`, `GetDueJobs`, `GetStaleClaims`, `MoveToDeadLetter`
   - `TryClaim` must use `SELECT ... FOR UPDATE SKIP LOCKED`
   - Write integration tests using `testcontainers-go` with a real Postgres instance

5. **Redis queue abstraction** (`internal/queue/`)
   - `Enqueue(jobID, priority)` — push to `queue:{priority}` list
   - `Dequeue(timeout)` — `BRPOPLPUSH` from queue to processing list
   - `Heartbeat(jobID, token, ttl)` — set heartbeat key with TTL
   - `RemoveFromProcessing(workerID, jobID)`
   - Write unit tests with a real Redis via testcontainers

6. **Worker** (`internal/worker/`, `cmd/worker/`)
   - Implement the exact loop from `architecture.md` (the pseudocode block)
   - claim token = `uuid.v4()`
   - Heartbeat goroutine: 5-second interval, 15-second TTL
   - On claim token mismatch at complete: log and discard, do not panic
   - For Phase 1, the only job type is `webhook` (HTTP POST to a URL in payload)
   - Graceful shutdown: drain in-flight jobs, respect a configurable timeout

7. **Scheduler** (`internal/scheduler/`, `cmd/scheduler/`)
   - No etcd yet. Single instance, no leader election.
   - Three loops, each in its own goroutine:
     - Due-job polling: every 100ms, call `GetDueJobs`, push to Redis
     - Visibility timeout reaping: every 5s, call `GetStaleClaims`, re-enqueue
     - Dead-letter promotion: every 30s, call jobs exceeding `max_retries`, move them
   - Cron expansion is Phase 2

8. **HTTP API** (`internal/api/`, `cmd/api/`)
   - Use `chi`
   - `POST /v1/jobs` — submit a job
   - `GET /v1/jobs/:id` — get job status
   - `GET /v1/jobs` — list jobs (with state filter, pagination)
   - `POST /v1/jobs/:id/cancel` — cancel a pending/scheduled job
   - No auth yet (add a placeholder middleware that always passes)
   - Persist to Postgres first, then enqueue to Redis for immediate jobs
   - Return 200 only after Postgres commit

9. **Single binary entrypoint** (`cmd/sluice/` or `main.go`)
   - `--role api | scheduler | worker`
   - Shared config struct loaded from env vars + optional config file

**Phase 1 is done when:**
- `docker-compose up` starts Postgres and Redis
- `go run ./cmd/... --role api` accepts job submissions
- `go run ./cmd/... --role worker` picks them up and executes them
- A crashed worker's job gets reassigned within 20 seconds
- A job exceeding max_retries lands in `dead_letter`
- Integration tests pass against real Postgres + Redis

---

### Phase 2 — Reliability + Scheduling

Start only when Phase 1 is complete.

1. **Idempotency keys** — `UNIQUE(tenant_id, idempotency_key)` is already in the schema. Wire it into `POST /v1/jobs`: if a duplicate key is submitted, return the existing job (not an error).
2. **Exponential backoff with jitter** — calculate next retry time in `internal/job/` as a pure function. Configurable per job.
3. **Cron scheduling** — parse cron expressions (`robfig/cron` or equivalent), expand schedules in the scheduler loop every 60s.
4. **Scheduled jobs** — `run_at` field already in schema. Scheduler's due-job poll handles these. Wire up `POST /v1/schedules`.
5. **`job_runs` partitioning** — add monthly partitions for `job_runs`. Write a migration.
6. **Auth** — JWT-based. Per-tenant API keys stored in a `tenants` table. Middleware reads the `Authorization` header and sets tenant context.
7. **Per-tenant rate limits** — token bucket in Redis at key `ratelimit:{tenant_id}`. Enforce in `POST /v1/jobs`.
8. **Admin CLI** (`cmd/sluice-cli/`) — `replay <job-id>`, `drain`, `force-fail <job-id>`, `dump-scheduler`

**Phase 2 is done when:**
- Duplicate idempotency keys return the original job, not a 409
- A job with `max_retries=3` retries 3 times with increasing delays before going to dead letter
- Cron schedules fire within 60 seconds of their due time
- API keys authenticate correctly; wrong key gets 401
- Rate-limited tenants get 429

---

### Phase 3 — High Availability

Start only when Phase 2 is complete.

1. **etcd leader election** — add etcd to `docker-compose.yml`. Scheduler acquires a 5-second TTL lease at `/sluice/scheduler/leader`. Renews every 1.5 seconds. Non-leaders are hot standbys: keep DB connection warm, do nothing else.
2. **Split-brain safety** — `GetDueJobs` already uses `SKIP LOCKED`. Verify this holds under dual-leader conditions with a test.
3. **Hot config reload** — `SIGHUP` reloads tenant configs and rate limits without restart.
4. **Weighted fair queuing** — within a priority lane, workers iterate tenants in proportion to their weight. Default weight 100.
5. **Graceful shutdown improvements** — workers complete in-flight jobs before exiting. API drains connections. Add configurable shutdown timeout.
6. **Kubernetes manifests** (`deploy/k8s/`, `deploy/helm/`) — API deployment (stateless), Scheduler deployment (leader-elected, 3 replicas), Worker deployment (HPA on queue depth).

**Phase 3 is done when:**
- Killing the scheduler leader causes a standby to take over within 2 seconds and jobs keep processing
- Hot config reload works without dropping connections
- Helm chart deploys successfully to a local k3s/minikube cluster

---

### Phase 4 — Observability + Dashboard

Start only when Phase 3 is complete.

1. **Prometheus metrics** — queue depth, processing latency histogram, retry counts, worker health, throughput per tenant. Expose `/metrics` on each role.
2. **Structured logs** — `log/slog` with correlation IDs threaded through context. Every state change logs at INFO with job ID and tenant ID.
3. **OpenTelemetry traces** — distributed traces from API submission to job completion. Export to Jaeger locally.
4. **Dashboard** (`web/`) — Next.js 14 + TypeScript + TanStack Query + Tailwind + shadcn/ui + Recharts. Pages: queue depth (live), recent runs, retry histories, dead-letter inspection. WebSocket subscription for live updates.

**Phase 4 is done when:**
- Prometheus scrapes all three roles
- Dashboard shows live queue state
- Traces are visible in Jaeger for a submitted job

---

## Guardrails — Read Before Writing Any Code

These exist because the most common failure mode is building the right thing in the wrong order, or building things that aren't needed yet.

### Things that are explicitly out of scope until their phase

- **gRPC**: not until Phase 4, maybe never. HTTP/REST is sufficient. Do not add protobuf or grpc dependencies before Phase 4.
- **etcd**: not until Phase 3. Scheduler runs as a single instance in Phases 1 and 2.
- **OpenTelemetry**: not until Phase 4. Use `log/slog` from day one, but no OTel SDK before Phase 4.
- **Kubernetes manifests**: not until Phase 3.
- **Next.js dashboard**: not until Phase 4. A `curl` command is enough to verify Phase 1-3.
- **sqlc**: optional. Use raw `pgx/v5` queries. Don't introduce sqlc unless the storage layer becomes obviously painful to maintain.
- **DAG jobs, WASM job types, Kafka source**: these are on the README roadmap but are not part of this build plan. Don't implement them.

### Patterns that must not change

These are core to correctness. Don't "simplify" them.

- **`TryClaim` uses `SELECT ... FOR UPDATE SKIP LOCKED`** — do not replace with application-level locking
- **Postgres commit before Redis enqueue** — never enqueue to Redis before the Postgres row is committed
- **Claim token checked on complete** — `CompleteJob` must verify the token matches; stale workers must be silently discarded, not panicked
- **Heartbeat runs in a separate goroutine** — do not inline it into the execution loop
- **Redis is reconstructible from Postgres** — never write to Redis data that doesn't exist in Postgres. Redis is a cache, not a source of truth.

### Scope creep signals — stop and reassess if you're about to

- Add a new dependency that wasn't in the README tech stack
- Write a new table that wasn't in the architecture doc's data model
- Build something for a phase that isn't the current phase
- Abstract something into a generic interface when there's only one implementation
- Write a helper that only has one caller and could just be inline
- Add a config option for behavior that should just always be one way

When you notice any of the above, stop. Ask whether it's actually needed now.

### Error handling rules

- Validate at boundaries (HTTP handler input, external API responses). Trust internal functions.
- A worker that can't connect to Redis should retry with backoff, not crash the process.
- A scheduler that loses the etcd lease should demote cleanly, not crash.
- A failed job should be recorded with the error in `last_error`, not swallowed.
- Do not add retries inside the storage layer for transient DB errors — let the caller decide.

---

## Code Conventions

- No comments unless the WHY is non-obvious (a timing invariant, a safety constraint, a workaround)
- No TODOs left in merged code
- No placeholder implementations — if a function can't be complete yet, don't create it
- Context propagates everywhere: every function that does I/O takes a `context.Context` as its first argument
- Errors wrap with `fmt.Errorf("...: %w", err)` — never discard errors
- Package names are singular: `job`, `queue`, `worker`, `scheduler`, `storage`, `tenant`
- Test file names: `foo_test.go` in the same package (white-box) or `foo_integration_test.go` for testcontainers tests

---

## File Structure (follow exactly)

```
sluice/
├── cmd/
│   ├── api/
│   ├── scheduler/
│   ├── worker/
│   └── sluice-cli/
├── internal/
│   ├── api/
│   ├── scheduler/
│   ├── worker/
│   ├── storage/
│   ├── queue/
│   ├── job/
│   ├── tenant/
│   ├── ratelimit/
│   └── telemetry/
├── migrations/
├── proto/
├── web/
├── deploy/
│   ├── docker/
│   ├── k8s/
│   └── helm/
├── docs/
└── scripts/
```

---

## Dev Environment

```bash
# Start dependencies
docker-compose up -d

# Run each role
go run ./cmd/... --role api
go run ./cmd/... --role scheduler
go run ./cmd/... --role worker

# Tests
make test          # unit + integration
make lint          # golangci-lint

# Migrations
make migrate-up
```

---

## Current Phase Tracker

Update this section at the end of each work session.

```
Phase 1 - Core Engine:        [x] COMPLETE — builds, migrations run, end-to-end test passed (job submitted → succeeded)
Phase 2 - Reliability:        [x] COMPLETE — auth 401/200, idempotency, jitter backoff, cron scheduling, rate limits, admin CLI
Phase 3 - High Availability:  [x] COMPLETE — etcd leader election, weighted fair queuing, SIGHUP reload, split-brain test, K8s + Helm
Phase 4 - Observability:      [x] COMPLETE — Prometheus (3/3 targets up), Jaeger traces (sluice-api + sluice-worker), Next.js dashboard live at :3030
```

Last worked on: 2026-09-23
Next task: All phases complete. Hardening pass done on branch `hardening` (see below). Optional: WASM job types, DAG support (see README roadmap).

Hardening pass (2026-09-23):
- Finished the Pulse→Sluice rename: Dockerfile, Helm helpers, K8s names (lowercase), env prefix is SLUICE_ (uppercase; Linux is case-sensitive)
- max_retries = retries after the first attempt (max_retries=3 → 4 executions); first retry waits backoff_seconds
- Workers drain in-flight jobs on SIGTERM; --shutdown-timeout aborts and records the attempt
- Stats/WebSocket endpoints are tenant-scoped; /ws authenticates via ?token=
- API keys stored as SHA-256 (migration 7); create/rotate via sluice-cli
- Webhooks refuse non-public addresses unless SLUICE_WEBHOOK_ALLOW_PRIVATE=true (dev/tests)
- Cancel sets state 'cancelled' (migration 6) instead of deleting
- Scheduler leader exports sluice_queue_depth; KEDA scales workers on it
- Workers run --concurrency jobs at once (default 10); dequeue blocks on BLMPOP
- Enqueue is idempotent via an enqueued:{job_id} marker (TTL queue.EnqueuedTTL), so
  reconciler/reaper re-enqueues don't duplicate waiting jobs; `sluice-cli drain` clears markers too

Dev notes:
- Postgres runs on port 5433 (native Postgres occupies 5432 on this machine)
- Docker trust auth used for local dev (POSTGRES_HOST_AUTH_METHOD=trust in docker-compose.yml)
- Race detector (-race) requires MinGW/GCC on Windows — skip locally, runs in CI
- Go binary: C:\Program Files\Go\bin\go
- migrate CLI installed with: go install -tags 'pgx5' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
- migrate URL format: pgx5://sluice:sluice@localhost:5433/sluice?sslmode=disable
- etcd runs on port 2379 (quay.io/coreos/etcd:v3.5.16), single-node for local dev
- Queue keys are now per-tenant: queue:{priority}:{tenantID} — flush Redis when switching from Phase 2 data
- Integration tests: make test-integration (needs Docker stack + migrations; uses Redis DB 15 and throwaway tenants)
- Race detector on Windows: run the tests in a golang:1.25 container pointed at host.docker.internal (SLUICE_TEST_POSTGRES_URL / SLUICE_TEST_REDIS_ADDR)
- Load test: scripts/loadtest (see README "Load testing")
- SIGHUP reloads tenant weights in worker: kill -SIGHUP <worker-pid> or Send-Signal on Windows
