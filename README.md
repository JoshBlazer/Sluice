# Sluice

> A horizontally scalable, durable job scheduler written in Go. At-least-once delivery, exponential backoff, leader-elected HA, and a real dashboard.

[![Go Version](https://img.shields.io/badge/go-1.25+-00ADD8?logo=go)](https://golang.org)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/JoshBlazer/Sluice/actions/workflows/ci.yml/badge.svg)](https://github.com/JoshBlazer/Sluice/actions/workflows/ci.yml)

Sluice is a from-scratch alternative to Sidekiq, Celery, or AWS SQS + EventBridge for teams that want the durability of a relational store, the throughput of an in-memory queue, and the operational simplicity of a single Go binary per role.

---

## Why Sluice?

Most teams reach for either a Redis-only queue (fast but loses jobs on crash) or a full workflow engine like Temporal (powerful but heavy). Sluice occupies the middle: Postgres as the durable source of truth, Redis as the hot path, and a clean separation between scheduling and execution.

- **Durable by default** — jobs survive crashes, network partitions, and worker death
- **High throughput** — designed for 10k+ jobs/sec on commodity hardware
- **At-least-once delivery** — with idempotency keys to deduplicate retries
- **Scheduler HA** — leader election via etcd, with hot standbys
- **Multi-tenant** — per-tenant rate limits and weighted fair queuing
- **Observable** — Prometheus metrics, OpenTelemetry traces, structured logs on every code path
- **Operable** — graceful shutdown, hot config reload, admin CLI for incident response

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

# Submit a job (dev-token is the seeded local tenant)
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

# Start the dashboard
cd web && npm install && npm run dev -- --port 3000
# open http://localhost:3000
```

Priority is an integer from `1` (highest) to `10` (lowest), in three lanes: `1` = high, `2`–`5` = normal, `6`–`10` = low. A high-priority job from any tenant runs before every normal one.

`max_retries` is the number of retries after the first attempt, so `max_retries: 3` means up to 4 executions. The first retry waits `backoff_seconds` (default 30), then doubles each time, capped at an hour, with ±20% jitter.

### Tenants and API keys

API keys are stored only as SHA-256 hashes. Create a tenant and get its key with the admin CLI:

```bash
go run ./cmd/sluice-cli create-tenant -rate-limit 200 -weight 100 acme
go run ./cmd/sluice-cli rotate-key <tenant-id>   # revokes the old key
```

`dev-token` is seeded by the migrations for local use only. Before any real deployment, disable it: `UPDATE tenants SET status = 'disabled' WHERE name = 'dev';`

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

    Worker1 -->|execute| Target[User Code /<br/>Webhook]

    Dashboard[Dashboard] -->|query| API

    Prometheus[Prometheus] -.scrape.-> API
    Prometheus -.scrape.-> Scheduler
    Prometheus -.scrape.-> Worker1
```

Full design and trade-offs are documented in [architecture.md](architecture.md).

---

## Features

### Job Types

| Type | Use Case | Example |
|------|----------|---------|
| Immediate | Run as soon as a worker is available | Webhook on order placed |
| Scheduled | Run at a specific future time | Send reminder at 9am tomorrow |
| Recurring | Run on a cron schedule | Nightly database cleanup |

### Reliability

- **At-least-once delivery** with visibility timeouts for crashed workers
- **Idempotency keys** — duplicate submissions return the original job
- **Exponential backoff with jitter** — configurable per job
- **Dead-letter queue** — jobs exhausting retries are quarantined for inspection
- **Worker heartbeats** — abandoned jobs return to the queue automatically

### Multi-Tenancy

- Per-tenant API keys
- Per-tenant rate limits (jobs/sec)
- Weighted fair queuing across tenants within each priority lane
- Per-tenant metrics

### Observability

- **Metrics**: queue depth, processing latency histogram, retry counts, worker health, throughput per tenant
- **Tracing**: distributed traces from API submission to job completion via OpenTelemetry + Jaeger
- **Logs**: structured JSON via `log/slog` with correlation IDs threaded through context
- **Dashboard**: real-time queue depth, recent runs, dead-letter inspection, scoped to the API key it runs with

### Operations

- **Graceful shutdown**: workers drain in-flight jobs before exiting (configurable timeout)
- **Hot config reload**: SIGHUP makes workers reload tenant weights immediately (they also refresh every 60s); rate-limit changes apply on the next request
- **Admin CLI**: create tenants, rotate keys, replay dead-letter jobs, drain queues, force-fail stuck jobs, dump scheduler state
- **Backup-friendly**: Postgres is the source of truth; standard backup tooling applies

---

## Performance Targets

Design targets on a 3-node cluster (4 vCPU / 8 GB RAM each), Postgres 16, Redis 7:

| Metric | Target |
|--------|--------|
| Submission throughput | 10,000+ jobs/sec |
| End-to-end latency (p50) | < 10 ms (submit → pickup) |
| End-to-end latency (p99) | < 50 ms (submit → pickup) |
| Scheduler failover | < 2 seconds (leader → hot standby) |
| Recovery from full node loss | < 30 seconds (all in-flight jobs) |

These are design targets, not measured results. `scripts/loadtest` measures submission throughput and submit→execute latency against a running stack; see [Load testing](#load-testing).

---

## Tech Stack

**Language & Runtime**
- Go 1.25+ with `log/slog` and context-aware everything

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

- Go 1.25+
- Docker + Docker Compose
- `migrate` CLI: `go install -tags 'pgx5' github.com/golang-migrate/migrate/v4/cmd/migrate@latest`
- Node 20+ (for the dashboard)

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

# Dashboard (uses dev-token unless NEXT_PUBLIC_API_TOKEN is set)
cd web && npm install && npm run dev
```

### Configuration

Every flag can also be set by environment variable:

| Variable | Flag | Default |
|----------|------|---------|
| `SLUICE_ROLE` | `--role` | (required) `api`, `scheduler` or `worker` |
| `SLUICE_POSTGRES_URL` | `--postgres-url` | `postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable` |
| `SLUICE_REDIS_ADDR` | `--redis-addr` | `localhost:6379` |
| `SLUICE_ETCD_ENDPOINTS` | `--etcd-endpoints` | `localhost:2379` |
| `SLUICE_OTLP_ENDPOINT` | `--otlp-endpoint` | `localhost:4318` |
| `SLUICE_PORT` | `--port` | `8080` (api) |
| `SLUICE_METRICS_PORT` | `--metrics-port` | `9091` (scheduler), `9092` (worker) |
| `SLUICE_WORKER_CONCURRENCY` | `--concurrency` | `10`. Jobs each worker process runs at once. Workers size their Postgres pool to match unless the URL sets `pool_max_conns` |
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

CI runs both with `-race` on every push and pull request, and there a missing stack is a failure rather than a skip (`SLUICE_TEST_REQUIRE_INFRA=1`). CI also builds the Docker image, lints and renders the Helm chart, and builds the dashboard.

### Load testing

```bash
go run ./cmd/sluice-cli create-tenant -rate-limit 0 loadtest   # note the key
# start api, scheduler and one or more workers with SLUICE_WEBHOOK_ALLOW_PRIVATE=true
go run ./scripts/loadtest -key <key> -n 20000 -c 64
```

It reports submission throughput, submit latency, and submit→execute latency percentiles. Execution throughput scales with worker replicas × `--concurrency`. The API's Postgres pool defaults to pgx's `max(4, NumCPU)` connections; under heavy submission load, raise it with `pool_max_conns` in `SLUICE_POSTGRES_URL`.

---

## Deployment

### Docker image

```bash
make docker-build     # sluice:dev, containing /sluice and /sluice-cli
docker run --rm -e SLUICE_POSTGRES_URL=... -e SLUICE_REDIS_ADDR=... sluice:dev --role worker
```

### Kubernetes

The chart expects Postgres, Redis and etcd to exist already, and migrations to have been applied.

```bash
helm install sluice deploy/helm \
  --set postgres.url="postgres://sluice:$PG_PASSWORD@postgres:5432/sluice?sslmode=require" \
  --set worker.replicas=10
```

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
