# Sluice in Codespaces

Everything is already running:
- the API, scheduler and worker
- Postgres, Redis, etcd, Jaeger and Prometheus (in Docker)
- a demo generator that keeps submitting jobs: some succeed, some fail and retry, and a few end in the dead letter

| Port | What |
|------|------|
| 3000 | **Dashboard.** Open it from the Ports tab if it didn't open by itself. The dev tenant's key (`dev-token`) is filled in; press Sign in |
| 16686 | Jaeger: traces that follow each job from `POST /v1/jobs` to its webhook call |
| 9090 | Prometheus: Sluice's metrics and alert rules |
| 8080 | The Sluice API |

Try it from the terminal:

```bash
# submit a job that fails twice, then lands in the dead letter
curl -X POST localhost:8080/v1/jobs -H "Authorization: Bearer dev-token" \
  -H "Content-Type: application/json" -d '{"type": "webhook", "payload": {"url": "http://127.0.0.1:9098/broken"}, "max_retries": 2, "backoff_seconds": 2}'

# watch the worker handle it
tail -f /tmp/sluice/worker.log
```

Everything logs to `/tmp/sluice/`. `bash .devcontainer/start.sh` restarts it all. To go further, see the [Quick Start](../README.md#quick-start) and the [docs](../docs).
