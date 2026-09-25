# Configuration

Every setting is a flag on the `sluice` binary and can also be set by environment variable. The role (`--role api|scheduler|worker`) decides which settings apply.

| Variable | Flag | Default |
|----------|------|---------|
| `SLUICE_ROLE` | `--role` | (required) `api`, `scheduler` or `worker` |
| `SLUICE_POSTGRES_URL` | `--postgres-url` | `postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable` |
| `SLUICE_REDIS_ADDR` | `--redis-addr` | `localhost:6379`. For auth, a database number or TLS, use a URL: `redis://user:pass@host:6379/0`, or `rediss://…` for TLS |
| `SLUICE_ETCD_ENDPOINTS` | `--etcd-endpoints` | `localhost:2379` (scheduler only) |
| `SLUICE_ETCD_USERNAME` / `SLUICE_ETCD_PASSWORD` | `--etcd-username` / `--etcd-password` | unset. etcd user auth |
| `SLUICE_ETCD_CA_FILE` | `--etcd-ca-file` | unset. CA that signs the etcd server certificate; enables TLS |
| `SLUICE_ETCD_CERT_FILE` / `SLUICE_ETCD_KEY_FILE` | `--etcd-cert-file` / `--etcd-key-file` | unset. Client certificate for etcd mutual TLS |
| `SLUICE_OTLP_ENDPOINT` | `--otlp-endpoint` | `localhost:4318`. OpenTelemetry traces (OTLP/HTTP) |
| `SLUICE_PORT` | `--port` | `8080` (api) |
| `SLUICE_METRICS_PORT` | `--metrics-port` | `9091` (scheduler), `9092` (worker). The API serves `/metrics` on its main port |
| `SLUICE_ADMIN_TOKEN` | `--admin-token` | unset. Enables the `/admin/v1` tenant-management API (api only; at least 32 characters). See [Operations](operations.md#admin-api) |
| `SLUICE_WORKER_CONCURRENCY` | `--concurrency` | `10`. Jobs each worker process runs at once |
| `SLUICE_RETENTION_DAYS` | `--retention-days` | `30` (scheduler). See [retention](#retention) |
| `SLUICE_WEBHOOK_ALLOW_PRIVATE` | `--webhook-allow-private` | `false`. When false, webhooks to loopback, private, link-local (cloud metadata) and other non-public addresses are refused. Only enable for local development |
| | `--shutdown-timeout` | `30s`. How long a stopping worker waits for in-flight jobs before aborting them (they're recorded as failed attempts and retried) |
| | `--version` | Print the build version and exit |

## Database connections

Workers size their Postgres pool to `concurrency + 4` connections. The API and scheduler use pgx's default, `max(4, NumCPU)`. Set `pool_max_conns` in `SLUICE_POSTGRES_URL` to override either, for example to give the API more connections under heavy submission load:

```
postgres://sluice:...@db:5432/sluice?sslmode=require&pool_max_conns=40
```

Keep the total across every API, worker and scheduler process under Postgres's `max_connections` (100 by default), or claims start failing with "too many clients". Workers recover from that (failed claims are requeued, outcome writes are retried), but throughput suffers.

## Retention

The scheduler leader deletes, in batches, once they are `SLUICE_RETENTION_DAYS` old:
- succeeded and cancelled jobs and their run history
- dead-letter entries and their jobs
- whole monthly `job_runs` partitions that lie entirely before the cutoff

Pending, scheduled, running and retrying jobs are never deleted. `0` keeps everything. A pruned job's idempotency key can be reused.

## Tenant caches

API replicas cache API key → tenant for 10 seconds, and workers refresh tenants every 5 seconds (or immediately on `SIGHUP`). So a rotated or disabled key, a changed rate limit, weight or concurrency cap, or a rotated webhook secret takes effect within those windows.
