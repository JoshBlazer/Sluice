# Operations

## Tenants and API keys

API keys are stored only as SHA-256 hashes. Create a tenant and get its key (printed once) with the admin CLI:

```bash
sluice-cli create-tenant -rate-limit 200 -weight 100 -max-concurrency 50 acme
```

| Limit | Meaning |
|-------|---------|
| `rate-limit` | Job submissions per second (token bucket). `0` = unlimited |
| `weight` | Share of worker capacity relative to other tenants, within a priority lane |
| `max-concurrency` | Most jobs the tenant can have running at once, across all workers. `0` = unlimited |

The `dev` tenant, with the public API key `dev-token`, exists only for local development. Migrations keep it disabled, and only `sluice-cli enable-dev-tenant` turns it on. Never run that against a shared or production database.

Changes to keys, limits and webhook secrets reach API replicas within 10 seconds and workers within 5 (see [tenant caches](configuration.md#tenant-caches)).

## Admin CLI

`sluice-cli` reads `SLUICE_POSTGRES_URL` and `SLUICE_REDIS_ADDR` like the server does. It is in the release image as `/sluice-cli`, or run it with `go run ./cmd/sluice-cli`.

```bash
# Tenants
sluice-cli create-tenant [-rate-limit N] [-weight N] [-max-concurrency N] <name>
sluice-cli set-tenant-limits [-rate-limit N] [-weight N] [-max-concurrency N] <tenant-id>
sluice-cli rotate-key <tenant-id>              # the old key stops working
sluice-cli rotate-webhook-secret <tenant-id>
sluice-cli enable-dev-tenant                   # local development only

# Incidents
sluice-cli replay <job-id>                     # re-enqueue a dead-letter job
sluice-cli force-fail <job-id>                 # move a stuck job to the dead letter now
sluice-cli drain                               # flush the Redis queues; jobs stay in Postgres and are re-enqueued
sluice-cli list-dead                           # 50 most recent dead-letter entries
sluice-cli queue-depth                         # per tenant and priority
sluice-cli dump-scheduler                      # job counts, active workers, upcoming schedules, queue depths
```

## Admin API

Setting `SLUICE_ADMIN_TOKEN` (at least 32 characters) on the API enables `/admin/v1`: the same tenant management over HTTP (create, list, get, change limits, disable or re-enable, rotate API keys and webhook secrets). It authenticates with `Authorization: Bearer <admin token>`, which tenant keys can never satisfy. Without the token, the admin routes answer 404. The endpoints are in the [OpenAPI spec](../internal/api/openapi.yaml).

```bash
curl -X POST http://localhost:8080/admin/v1/tenants \
  -H "Authorization: Bearer $SLUICE_ADMIN_TOKEN" \
  -d '{"name": "acme", "rate_limit": 200, "max_concurrency": 50}'

# Disable a tenant: its key stops working within 10s; nothing is deleted
curl -X PATCH http://localhost:8080/admin/v1/tenants/<id> \
  -H "Authorization: Bearer $SLUICE_ADMIN_TOKEN" \
  -d '{"status": "disabled"}'
```

## Running processes

- **Graceful shutdown.** On `SIGTERM` a worker stops claiming, finishes in-flight jobs, and after `--shutdown-timeout` aborts any still running; those are recorded as failed attempts and retried.
- **Reload.** `SIGHUP` makes a worker reload tenants immediately instead of waiting for its 5-second refresh.
- **Scheduler failover.** Standby schedulers take over within ~50 ms when the leader shuts down cleanly, and within 1.5–2.6 s when it crashes (bounded by the etcd lease).
- **Storage.** Old finished jobs, run history and dead letters are pruned automatically; see [retention](configuration.md#retention).

## Observability

- **Metrics.** Every role exports Prometheus metrics: queue depth, submit→execute and execution latency histograms, retries, dead-lettering, worker health, and throughput per tenant. The API serves `/metrics` on its main port; schedulers and workers on `--metrics-port`.
- **Tracing.** OpenTelemetry traces go to `SLUICE_OTLP_ENDPOINT` (Jaeger in the local stack, at http://localhost:16686). The trace context of the submitting request is stored with the job, so a job's `worker.execute` span, and the webhook call inside it, belong to the same trace as its `POST /v1/jobs`, even when the job runs hours later. An incoming W3C `traceparent` header is honoured, so the trace can start in your own service.
- **Logs.** Structured JSON via `log/slog`; job-related lines carry the job ID.
- **Alerts.** [Prometheus rules](../deploy/helm/files/alerts.yml) cover a missing or duplicated scheduler leader, a growing backlog, job failure spikes, dead-lettering, API errors and latency, throttled tenants and down targets. They're unit-tested with `promtool` in CI ([tests](../deploy/monitoring/alerts_test.yml)). Plain Prometheus can load the file via `rule_files`, as the local docker-compose setup does.
- **Grafana.** Import [`deploy/monitoring/grafana-dashboard.json`](../deploy/monitoring/grafana-dashboard.json) for queue depth, throughput, failure rate, job and API latency, per-worker load and per-tenant views.

## Dashboard

The Next.js dashboard in `web/` shows live queue depth, recent runs, per-job attempt timelines, schedules and dead-letter inspection with replay. Viewers sign in with a tenant API key and see only that tenant. No key is built into the dashboard, and a signed-in key is kept only for the browser tab.

```bash
cd web && npm install && npm run dev -- --port 3000
```

Set `NEXT_PUBLIC_API_URL` if the API isn't at `http://localhost:8080`. If the browser can reach only the dashboard, as in GitHub Codespaces, set `SLUICE_API_PROXY=http://localhost:8080` and `NEXT_PUBLIC_API_URL=/sluice-api`. The dashboard then serves the API, including the live WebSocket, from its own origin.

To see the dashboard with something happening, `go run ./scripts/demo -key dev-token` submits a steady mix of jobs: some succeed, some fail and retry, a few end in the dead letter, and occasional bursts build a backlog. It also adds a once-a-minute cron schedule. The worker needs `SLUICE_WEBHOOK_ALLOW_PRIVATE=true`, because the demo serves the webhooks itself.
