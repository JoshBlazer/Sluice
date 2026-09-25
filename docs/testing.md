# Testing

## Unit and integration tests

1. **Unit tests** need no I/O: `make test-unit`.
2. **Integration tests** (`-tags integration`) run the API, worker, scheduler loops, queue and storage against real Postgres, Redis and etcd. They cover retries into the dead letter, crashed-worker recovery, graceful drain, scheduler failover, Redis loss, tenant isolation, rate and concurrency limits, cron deduplication and trace propagation. They use Redis DB 15 and throwaway tenants, so they don't disturb local dev data, and they skip if the stack isn't up:

   ```bash
   make up && make migrate-up && make test-integration
   ```

CI runs both with `-race` on every push and pull request. There, a missing stack is a failure rather than a skip (`SLUICE_TEST_REQUIRE_INFRA=1`). CI also:
- builds the Docker image
- lints, renders and installs the Helm chart on kind, then runs a job through it
- unit-tests the alert rules with `promtool`
- builds the dashboard
- fails on known vulnerabilities (`govulncheck` for Go, `npm audit` for the dashboard's production dependencies)

A test also fails if any API route is missing from the OpenAPI spec. Dependabot proposes dependency updates weekly.

## Failure-mode (chaos) tests

The [Chaos workflow](../.github/workflows/chaos.yml) ([`scripts/chaos.sh`](../scripts/chaos.sh)) runs weekly and on PRs touching the job pipeline. It injects a fault while 3,000 jobs are in flight and checks that every accepted job still executes. Recent results on a GitHub runner:

| Fault | Accepted jobs executed | Duplicate executions | Drained after the fault |
|---|---|---|---|
| Worker `kill -9` | 3,000 / 3,000 | 2–19 | ~20 s |
| Scheduler leader `kill -9` | 3,000 / 3,000 | 0 | ~11 s |
| Redis loses all data (`FLUSHALL`) | 3,000 / 3,000 | 0 | ~42 s |
| Redis restart | 3,000 / 3,000 | 0 | ~11 s |
| Postgres restart | 3,000 / 3,000 | 0 | ~11 s |
| Every Sluice process `kill -9`, then restarted | 3,000 / 3,000 | 0 | ~12.5 s |

Duplicates are at-least-once re-runs of jobs whose worker died mid-request; receivers deduplicate them by the `webhook-id` header. Whether any occur depends on how many requests were in flight at the moment of the kill.

## Load testing

```bash
go run ./cmd/sluice-cli create-tenant -rate-limit 0 loadtest   # note the key
# start api, scheduler and one or more workers with SLUICE_WEBHOOK_ALLOW_PRIVATE=true
go run ./scripts/loadtest -key <key> -n 20000 -c 64
```

It reports submission throughput, submit latency, submit→execute latency percentiles and duplicate deliveries. Execution throughput scales with worker replicas × `--concurrency`. Under heavy submission load, give the API more database connections (see [Configuration](configuration.md#database-connections)).

The [Benchmark workflow](../.github/workflows/benchmark.yml) runs this against a full stack on a GitHub runner; results are summarised in the [README](../README.md#performance).
