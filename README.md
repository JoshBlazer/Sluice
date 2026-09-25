# Sluice

> A horizontally scalable, durable job scheduler written in Go. At-least-once delivery, exponential backoff, leader-elected HA, and a real dashboard.

[![Go Version](https://img.shields.io/badge/go-1.26+-00ADD8?logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/JoshBlazer/Sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/JoshBlazer/Sluice/actions/workflows/ci.yml)

Sluice is a from-scratch alternative to Sidekiq, Celery, or AWS SQS + EventBridge for teams that want the durability of a relational store, the throughput of an in-memory queue, and the operational simplicity of a single Go binary per role.

---

## Why Sluice?

Most teams reach for either a Redis-only queue (fast but loses jobs on crash) or a full workflow engine like Temporal (powerful but heavy). Sluice occupies the middle: Postgres as the durable source of truth, Redis as the hot path, and a clean separation between scheduling and execution.

- **Redis is just a cache** — the API answers only after the job is committed to Postgres. Flush or lose Redis entirely and the scheduler rebuilds the queues from Postgres; in the chaos tests, 3,000 in-flight jobs all ran within 40 seconds of Redis losing its data
- **Durable by default** — jobs survive crashes, network partitions, and worker death. A crashed worker's job is picked up again within 20 seconds
- **At-least-once delivery, without double-writes** — a stalled worker that wakes up after its job was reassigned can't overwrite the new result, and idempotency keys deduplicate submissions
- **Scheduler HA** — leader election via etcd with hot standbys. Failover takes ~50ms on shutdown and ~2s (up to ~2.6s) after a crash, and two leaders overlapping briefly can't double-run anything
- **Multi-tenant and secure** — per-tenant rate limits, weighted fair queuing, isolated data, hashed API keys, signed webhooks ([Standard Webhooks](https://www.standardwebhooks.com/)), and webhooks that refuse to call internal addresses
- **High throughput** — designed for 10k+ jobs/sec; workers run many jobs concurrently
- **Observable** — Prometheus metrics, OpenTelemetry traces, structured logs on every code path
- **Operable** — graceful shutdown that finishes in-flight jobs, hot config reload, admin CLI for incident response

The durability, failover, isolation and security behaviour above is covered by tests that CI runs on every push, with the race detector on, against real Postgres, Redis and etcd. Throughput is a design target; see [Performance](#performance-targets).

---

## Quick Start

```bash
# Spin up Postgres, Redis, etcd, Jaeger, and Prometheus, then create the schema
docker compose up -d
make migrate-up

# Run each role (separate terminals). The dev worker flag lets webhooks
# reach localhost; production workers refuse private addresses.
make dev-api
make dev-scheduler
SLUICE_WEBHOOK_ALLOW_PRIVATE=true make dev-worker

# Enable the local-only "dev" tenant (API key: dev-token). Migrations disable it
# on every database, so it never works anywhere you haven't run this.
go run ./cmd/sluice-cli enable-dev-tenant

# Submit a job
curl -X POST http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "webhook",
    "payload": {"url": "https://example.com/hook", "method": "POST"},
    "max_retries": 5,
    "priority": 1,
    "idempotency_key": "order-1234"
  }'

# Schedule a job for the future
curl -X POST http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "webhook",
    "payload": {"url": "https://example.com/reminder"},
    "run_at": "2030-01-15T10:00:00Z"
  }'

# Register a recurring job
curl -X POST http://localhost:8080/v1/schedules \
  -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "nightly-cleanup",
    "cron": "0 2 * * *",
    "timezone": "Europe/London",
    "job_template": {
      "type": "webhook",
      "payload": {"url": "https://example.com/cleanup"}
    }
  }'

# Start the dashboard, then sign in with an API key (dev-token locally)
cd web && npm install && npm run dev -- --port 3000
# open http://localhost:3000
```

Priority is an integer from `1` (highest) to `10` (lowest), in three lanes: `1` = high, `2`–`5` = normal, `6`–`10` = low. A high-priority job from any tenant runs before every normal one.

`max_retries` is the number of retries after the first attempt, so `max_retries: 3` means up to 4 executions. The first retry waits `backoff_seconds` (default 30), then doubles each time, capped at an hour, with ±20% jitter.

### Tenants and API keys

API keys are stored only as SHA-256 hashes. Create a tenant and get its key with the admin CLI:

```bash
go run ./cmd/sluice-cli create-tenant -rate-limit 200 -weight 100 -max-concurrency 50 acme
go run ./cmd/sluice-cli rotate-key <tenant-id>   # revokes the old key (API replicas cache keys for up to 10s)
```

The `dev` tenant, with the public API key `dev-token`, exists only for local development. Migrations keep it disabled, and only `sluice-cli enable-dev-tenant` turns it on. Never run that against a shared or production database.

---

## Architecture at a Glance

```mermaid
flowchart LR
    Client[Client / Producer] -->|HTTP| API[API Gateway]
    API -->|persist| PG[(Postgres)]
    API -->|enqueue| Redis[(Redis)]

    Scheduler[Scheduler<br/>Leader-Elected] -->|poll due jobs| PG
    Scheduler -->|push to queue| Redis
    Scheduler -.->|leader election| Etcd[(etcd)]

    Worker1[Worker] -->|pull| Redis
    Worker2[Worker] -->|pull| Redis
    WorkerN[Worker N] -->|pull| Redis

    Worker1 -->|update status| PG
    Worker2 -->|update status| PG
    WorkerN -->|update status| PG

    Worker1 -->|HTTP call| Target[Webhook<br/>endpoint]

    Dashboard[Dashboard] -->|query| API

    Prometheus[Prometheus] -.scrape.-> API
    Prometheus -.scrape.-> Scheduler
    Prometheus -.scrape.-> Worker1
```

Full design and trade-offs are documented in [architecture.md](architecture.md).

---

## Features

### Jobs

Jobs are **webhooks**: an HTTP request (`GET`, `POST`, `PUT`, `PATCH` or `DELETE`) with optional headers and body. Any status below 400 counts as success. `timeout_seconds` (1–900, default 25) bounds each attempt. Every request is signed so the receiver can verify it came from Sluice (see [Verifying webhooks](#verifying-webhooks)). Each job runs on one of three timings:

| Timing | Use Case | Example |
|--------|----------|---------|
| Immediate | Run as soon as a worker is available | Webhook on order placed |
| Scheduled (`run_at`) | Run at a specific future time | Send reminder at 9am tomorrow |
| Recurring (`/v1/schedules`) | Run on a cron schedule, in the schedule's timezone. `PATCH` pauses (`"enabled": false`), resumes or edits it; resuming skips occurrences missed while paused | Nightly database cleanup |

### Reliability

- **Postgres is the source of truth** — Redis only holds queue order. Anything lost from Redis is rebuilt from Postgres
- **At-least-once delivery** — heartbeats extend each job's claim; if a worker dies, its job is reclaimed within 20 seconds
- **Stale workers can't clobber results** — every claim carries a token, and a worker whose job was reassigned has its result discarded
- **Safe under split brain** — jobs are claimed with `SELECT … FOR UPDATE SKIP LOCKED`, and every scheduler task is safe to run twice, so a brief dual leader can't double-run a job
- **Idempotency keys** — duplicate submissions return the original job, and cron occurrences can't fire twice
- **Exponential backoff with jitter** — configurable per job
- **Dead-letter queue** — jobs exhausting retries are quarantined for inspection and one-click replay

### Multi-Tenancy

- Per-tenant API keys
- Per-tenant rate limits (token bucket, jobs/sec)
- Per-tenant concurrency limits (`max_concurrency`): a capped tenant never has more jobs running than its cap, even with many workers claiming at once, and a busy tenant can't occupy every worker slot
- Strict priority lanes across tenants (any tenant's urgent job runs before everything normal), with weighted fair queuing between tenants inside each lane
- Every API, stats and dashboard view is scoped to the caller's tenant
- Per-tenant metrics

### Security

- **SSRF protection** — webhooks refuse loopback, private, link-local (e.g. cloud metadata at `169.254.169.254`) and other non-public addresses. The check runs on the resolved IP when connecting, so it also blocks redirects and DNS rebinding
- **Hashed API keys** — only SHA-256 digests are stored; `sluice-cli` issues and rotates keys
- **Signed webhooks** — every request carries [Standard Webhooks](https://www.standardwebhooks.com/) headers (`webhook-id`, `webhook-timestamp`, `webhook-signature`, an HMAC-SHA256 with the tenant's secret), which a job's own headers can't override

### Verifying webhooks

Each tenant has a signing secret (`whsec_…`), shown by `sluice-cli create-tenant` and available from `GET /v1/webhook-secret`. Verify requests with any [Standard Webhooks library](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries), for example in Node:

```js
import { Webhook } from "standardwebhooks";

const wh = new Webhook(process.env.SLUICE_WEBHOOK_SECRET);
// Throws if the signature is invalid or the timestamp is too old.
const payload = wh.verify(rawBody, request.headers);
```

`webhook-id` is the job ID and stays the same across retries, so receivers can use it to deduplicate at-least-once deliveries. `sluice-cli rotate-webhook-secret <tenant-id>` issues a new secret; workers switch to it within 5 seconds (or immediately after SIGHUP), so accept both secrets briefly while rotating.
- **Validated input** — job payloads, priorities, retry limits, cron templates and request sizes are checked at the API boundary

### Observability

- **Metrics**: queue depth, processing latency histogram, retry counts, worker health, throughput per tenant
- **Tracing**: distributed traces from API submission to job completion via OpenTelemetry + Jaeger
- **Logs**: structured JSON via `log/slog` with correlation IDs threaded through context
- **Alerts**: [Prometheus rules](deploy/helm/files/alerts.yml) for a missing or duplicated scheduler leader, a growing backlog, job failure spikes, dead-lettering, API errors and latency, throttled tenants and down targets. They're unit-tested with `promtool` in CI. The Helm chart can install them as a `PrometheusRule` plus a `PodMonitor` (`monitoring.*` values); plain Prometheus can load the file via `rule_files`, as the local docker-compose setup does
- **Grafana**: import [`deploy/monitoring/grafana-dashboard.json`](deploy/monitoring/grafana-dashboard.json) for queue depth, throughput, failure rate, job and API latency, per-worker load and per-tenant views
- **API reference**: an [OpenAPI 3.1 spec](internal/api/openapi.yaml), also served by every API replica at `/openapi.yaml`. A test fails if any route is missing from it
- **Dashboard**: real-time queue depth, recent runs, retry histories (per-job attempt timelines), dead-letter inspection. Viewers sign in with a tenant API key and see only that tenant. No key is built into the dashboard, and a signed-in key is kept only for the browser tab

### Operations

- **Concurrent workers**: each worker process runs many jobs at once (`--concurrency`, default 10), sizing its database pool to match
- **Graceful shutdown**: workers drain in-flight jobs before exiting; any still running at the timeout are aborted and recorded as failed attempts
- **Hot config reload**: SIGHUP makes workers reload tenant weights immediately (they also refresh every 5s, so new tenants are served within seconds); rate-limit changes apply on the next request
- **Admin CLI**: create tenants, rotate keys, replay dead-letter jobs, drain queues, force-fail stuck jobs, dump scheduler state
- **Bounded storage**: finished jobs, run history and dead letters are pruned after a configurable retention period (default 30 days), in small batches
- **Backup-friendly**: Postgres is the source of truth; standard backup tooling applies

---

## Performance Targets

Design targets are for a 3-node cluster (4 vCPU / 8 GB RAM each), Postgres 16, Redis 7. Measurements come from the [Benchmark workflow](.github/workflows/benchmark.yml), which runs Postgres, Redis, etcd and every Sluice role together on **one** 4-vCPU GitHub-hosted runner (a quarter of the target hardware), plus failover and recovery checks in CI:

| Metric | Target (3 nodes) | Measured (1 shared 4-vCPU runner) |
|--------|--------|----------|
| Submission throughput | 10,000+ jobs/sec | 1,550–3,500 jobs/sec; not yet run on target hardware |
| Execution throughput | — | 1,200–2,500 jobs/sec (20,000 jobs, 0 duplicate executions) |
| Latency p50 (submit → execute) | < 10 ms | 0.6–1.0 ms |
| Latency p99 (submit → execute) | < 50 ms | 2.8–8 ms (one noisy run: 65 ms) |
| Scheduler failover | < 2 seconds | ~50 ms on shutdown; 1.5–2.6 s after a crash, bounded by etcd lease expiry (checked in CI) |
| Worker crash recovery | — | < 20 seconds (checked in CI) |
| Recovery from full node loss | < 30 seconds | 17 s: every Sluice process killed with 3,000 jobs in flight, none lost ([chaos tests](#failure-mode-tests)) |

Throughput is given as a range because GitHub assigns runners with different CPU models, and the same code measures up to 2x apart between them; the benchmark now logs which CPU it ran on. `scripts/loadtest` reproduces the throughput and latency measurements against any running stack; see [Load testing](#load-testing).

---

## Tech Stack

**Language & Runtime**
- Go 1.26+ with `log/slog` and context-aware everything

**APIs & Communication**
- HTTP/REST via `chi`
- WebSocket subscriptions for dashboard live updates

**Storage & Coordination**
- PostgreSQL 16 (durable source of truth) via `pgx/v5`
- Redis 7 (hot queue) via `go-redis/v9`
- etcd v3 (leader election)

**Observability**
- Prometheus client library with custom collectors
- OpenTelemetry SDK exporting to Jaeger
- Structured JSON logs via `log/slog`

**Frontend**
- Next.js 14 + TypeScript
- TanStack Query
- Tailwind CSS + shadcn/ui
- Recharts

**Testing**
- `testing` (standard library) for unit tests
- Integration tests (`-tags integration`) dial the docker-compose Postgres/Redis directly and skip if not reachable

**Deployment**
- Docker multi-stage builds
- Docker Compose for local development
- Kubernetes manifests + Helm chart
- GitHub Actions CI

---

## Project Structure

```
sluice/
├── cmd/
│   ├── sluice/            # Single binary — run with --role api|scheduler|worker
│   └── sluice-cli/        # Admin CLI
├── internal/
│   ├── api/              # HTTP handlers, middleware, WebSocket
│   ├── scheduler/        # Scheduling, cron, leader election
│   ├── worker/           # Worker loop, executor, heartbeats
│   ├── storage/          # Postgres queries (hand-written pgx)
│   ├── queue/            # Redis queue abstraction
│   ├── job/              # Domain types, state machine
│   ├── tenant/           # Multi-tenancy context
│   ├── ratelimit/        # Token-bucket rate limiter per tenant
│   ├── metrics/          # Prometheus collectors
│   ├── leader/           # etcd leader election
│   ├── testutil/         # Integration-test helpers
│   └── telemetry/        # Tracing and logging
├── migrations/           # SQL migrations (golang-migrate)
├── web/                  # Next.js dashboard
├── scripts/loadtest/     # Load generator for the performance targets
├── deploy/
│   ├── docker/
│   ├── k8s/
│   └── helm/
└── architecture.md       # Detailed design doc
```

---

## Development

### Prerequisites

- Go 1.26+
- Docker + Docker Compose
- `migrate` CLI: `go install -tags 'pgx5' github.com/golang-migrate/migrate/v4/cmd/migrate@latest`
- Node 24 LTS (for the dashboard)

### Local Setup

```bash
# Clone and install tools
git clone https://github.com/JoshBlazer/Sluice
cd Sluice
make bootstrap        # installs migrate, downloads Go modules

# Start infrastructure
make up               # postgres, redis, etcd, jaeger, prometheus

# Apply migrations
make migrate-up

# Run each role in separate terminals
make dev-api
make dev-scheduler
SLUICE_WEBHOOK_ALLOW_PRIVATE=true make dev-worker

# Dashboard: sign in with a tenant API key. Set NEXT_PUBLIC_API_URL if the API
# isn't at http://localhost:8080.
cd web && npm install && npm run dev
```

### Configuration

Every flag can also be set by environment variable:

| Variable | Flag | Default |
|----------|------|---------|
| `SLUICE_ROLE` | `--role` | (required) `api`, `scheduler` or `worker` |
| `SLUICE_POSTGRES_URL` | `--postgres-url` | `postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable` |
| `SLUICE_REDIS_ADDR` | `--redis-addr` | `localhost:6379`. For auth, a database number or TLS, use a URL: `redis://user:pass@host:6379/0`, or `rediss://…` for TLS |
| `SLUICE_ETCD_ENDPOINTS` | `--etcd-endpoints` | `localhost:2379` (scheduler role only) |
| `SLUICE_ETCD_USERNAME` / `SLUICE_ETCD_PASSWORD` | `--etcd-username` / `--etcd-password` | unset. etcd user auth |
| `SLUICE_ETCD_CA_FILE` | `--etcd-ca-file` | unset. CA that signs the etcd server certificate; enables TLS |
| `SLUICE_ETCD_CERT_FILE` / `SLUICE_ETCD_KEY_FILE` | `--etcd-cert-file` / `--etcd-key-file` | unset. Client certificate for etcd mutual TLS |
| `SLUICE_OTLP_ENDPOINT` | `--otlp-endpoint` | `localhost:4318` |
| `SLUICE_PORT` | `--port` | `8080` (api) |
| `SLUICE_METRICS_PORT` | `--metrics-port` | `9091` (scheduler), `9092` (worker) |
| `SLUICE_WORKER_CONCURRENCY` | `--concurrency` | `10`. Jobs each worker process runs at once. Workers size their Postgres pool to `concurrency + 4` unless the URL sets `pool_max_conns`. Keep the total across all API, worker and scheduler processes under Postgres's `max_connections` (100 by default) |
| `SLUICE_RETENTION_DAYS` | `--retention-days` | `30`. The scheduler deletes succeeded and cancelled jobs, their run history and dead-letter entries once they are this old, and drops expired monthly `job_runs` partitions. Pending, scheduled, running and retrying jobs are never deleted. `0` keeps everything. A pruned job's idempotency key can be reused |
| `SLUICE_WEBHOOK_ALLOW_PRIVATE` | `--webhook-allow-private` | `false`. When false, webhooks to loopback, private, link-local (cloud metadata) and other non-public addresses are refused |
| | `--shutdown-timeout` | `30s`. How long a stopping worker waits for its in-flight job before aborting it |

### Common Tasks

```bash
make test             # unit + integration tests
make lint             # go vet
make migrate-up       # apply DB migrations
make docker-build     # build the Docker image
```

---

## Testing

Two tiers:

1. **Unit tests** — no I/O. `make test-unit`
2. **Integration tests** (`-tags integration`) — run the API, worker, scheduler loops, queue and storage against real Postgres and Redis: retries into dead letter, crashed-worker recovery, graceful drain, tenant isolation, rate limits, cron dedup. They use Redis DB 15 and throwaway tenants, so they don't disturb local dev data, and skip if the stack isn't up. `make up && make migrate-up && make test-integration`

CI runs both with `-race` on every push and pull request, and there a missing stack is a failure rather than a skip (`SLUICE_TEST_REQUIRE_INFRA=1`). CI also builds the Docker image, lints and renders the Helm chart, builds the dashboard, and fails on known vulnerabilities (`govulncheck` for Go, `npm audit` for the dashboard's production dependencies). Dependabot proposes dependency updates weekly.

### Failure-mode tests

The [Chaos workflow](.github/workflows/chaos.yml) ([`scripts/chaos.sh`](scripts/chaos.sh)) runs weekly and on PRs touching the job pipeline. It injects a fault while 3,000 jobs are in flight and checks that every accepted job still executes. Latest results on a GitHub runner:

| Fault | Accepted jobs executed | Duplicate executions | Drained after the fault |
|---|---|---|---|
| Worker `kill -9` | 3,000 / 3,000 | 0 | 19 s |
| Scheduler leader `kill -9` | 3,000 / 3,000 | 0 | 14 s |
| Redis loses all data (`FLUSHALL`) | 3,000 / 3,000 | 0 | 38 s |
| Redis restart | 3,000 / 3,000 | 0 | 12 s |
| Postgres restart | 3,000 / 3,000 | 0 | 13 s |
| Every Sluice process `kill -9`, then restarted | 3,000 / 3,000 | 24 | 17 s |

Duplicates are at-least-once re-runs of jobs whose worker died mid-request. Receivers deduplicate them by the `webhook-id` header.

### Load testing

```bash
go run ./cmd/sluice-cli create-tenant -rate-limit 0 loadtest   # note the key
# start api, scheduler and one or more workers with SLUICE_WEBHOOK_ALLOW_PRIVATE=true
go run ./scripts/loadtest -key <key> -n 20000 -c 64
```

It reports submission throughput, submit latency, and submit→execute latency percentiles. Execution throughput scales with worker replicas × `--concurrency`. The API's Postgres pool defaults to pgx's `max(4, NumCPU)` connections; under heavy submission load, raise it with `pool_max_conns` in `SLUICE_POSTGRES_URL`.

---

## Deployment

### Releases

Pushing a tag like `v0.4.0` runs the [Release workflow](.github/workflows/release.yml), which publishes:
- multi-arch (amd64, arm64) images: `ghcr.io/joshblazer/sluice:0.4.0` and `:latest`
- the Helm chart, versioned to match, attached to a GitHub Release with generated notes

Each image contains `/sluice`, `/sluice-cli`, the `migrate` CLI and the matching migrations in `/migrations`. `sluice --version` prints the build.

### Docker image

```bash
docker run --rm -e SLUICE_POSTGRES_URL=... -e SLUICE_REDIS_ADDR=... ghcr.io/joshblazer/sluice:latest --role worker
# apply the schema that ships with the image
docker run --rm --entrypoint /migrate ghcr.io/joshblazer/sluice:latest -path /migrations -database "$SLUICE_POSTGRES_URL" up
```

`make docker-build` builds the same image locally as `sluice:dev`.

### Kubernetes

The chart expects Postgres, Redis and etcd to exist already. It applies database migrations itself, as a pre-install/pre-upgrade hook job using the image being deployed (set `migrations.enabled=false` to manage the schema yourself). The API has readiness (`/readyz`, which checks Postgres and Redis) and liveness probes; schedulers and workers have liveness probes on their metrics port. CI installs the chart on a throwaway [kind](https://kind.sigs.k8s.io/) cluster and runs a job through it on every push ([`scripts/k8s-smoke.sh`](scripts/k8s-smoke.sh), with test dependencies in `deploy/kind/deps.yaml`).

```bash
helm install sluice deploy/helm \
  --set postgres.url="postgres://sluice:$PG_PASSWORD@postgres:5432/sluice?sslmode=require" \
  --set redis.addr="rediss://:$REDIS_PASSWORD@redis:6380/0" \
  --set worker.replicas=10
```

Connection strings and passwords go into a Kubernetes Secret, never the ConfigMap. To use a Secret you manage yourself, set `existingSecret` to its name (keys `SLUICE_POSTGRES_URL`, `SLUICE_REDIS_ADDR`, optionally `SLUICE_ETCD_PASSWORD`). For etcd over TLS, set `etcd.tls.secretName` to a Secret holding `ca.crt` (plus `tls.crt`/`tls.key` with `etcd.tls.clientCert=true`); it is mounted into scheduler pods.

Worker autoscaling uses a KEDA `ScaledObject` on `sum(sluice_queue_depth)`, which the scheduler leader exports. It needs KEDA installed and a Prometheus that scrapes the scheduler (`worker.autoscaling.prometheusAddress`); set `worker.autoscaling.enabled=false` otherwise.

Recommended production layout:

- 2× API replicas (stateless, load-balanced)
- 3× Scheduler replicas (1 leader + 2 hot standbys via etcd lease)
- 10× Worker replicas (scaled by KEDA on queue depth)
- 1× Postgres primary + 1 replica
- 1× Redis with persistence + 1 replica
- 3× etcd nodes

---

## Admin CLI

```bash
# Re-enqueue a dead-letter job
sluice-cli replay <job-id>

# Mark a stuck job as dead immediately
sluice-cli force-fail <job-id>

# Flush all Redis queues (jobs stay in Postgres, reconciler re-enqueues when ready)
sluice-cli drain

# Show job counts, active workers, upcoming schedules, and queue depths
sluice-cli dump-scheduler

# List the 50 most recent dead-letter entries
sluice-cli list-dead

# Queue depth per tenant and priority
sluice-cli queue-depth

# Create a tenant (prints its API key once) / replace a tenant's key
sluice-cli create-tenant [-rate-limit N] [-weight N] <name>
sluice-cli rotate-key <tenant-id>
sluice-cli rotate-webhook-secret <tenant-id>

# Change limits (API replicas and workers pick changes up within seconds)
sluice-cli set-tenant-limits -max-concurrency 20 -rate-limit 500 <tenant-id>
```

### Admin API

Setting `SLUICE_ADMIN_TOKEN` (at least 32 characters) on the API enables `/admin/v1`, the same tenant management over HTTP: create, list, get, change limits, disable or re-enable, rotate API keys and webhook secrets. It authenticates with `Authorization: Bearer <admin token>`, which tenant keys can never satisfy. Without the token the admin routes answer 404. See the [OpenAPI spec](internal/api/openapi.yaml) for the endpoints.

```bash
curl -X POST http://localhost:8080/admin/v1/tenants -H "Authorization: Bearer $SLUICE_ADMIN_TOKEN"   -d '{"name": "acme", "rate_limit": 200, "max_concurrency": 50}'
curl -X PATCH http://localhost:8080/admin/v1/tenants/<id> -H "Authorization: Bearer $SLUICE_ADMIN_TOKEN"   -d '{"status": "disabled"}'   # key stops working within 10s; nothing is deleted
```

---

## Roadmap

- [x] Core API, scheduler, worker with at-least-once delivery
- [x] Cron + delayed jobs
- [x] Leader election + scheduler HA
- [x] Multi-tenancy with weighted fair queuing
- [x] Dashboard with real-time updates
- [x] OpenTelemetry tracing
- [ ] Job DAGs with dependency resolution
- [ ] WASM-based custom job types (sandboxed user code)
- [ ] Kafka source for event-driven job submission
- [ ] Native Kubernetes operator

---

## License

MIT — see [LICENSE](LICENSE).
