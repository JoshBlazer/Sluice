# Deployment

## Releases

Pushing a tag like `v0.4.0` runs the [Release workflow](../.github/workflows/release.yml), which publishes:
- multi-arch (amd64, arm64) images: `ghcr.io/joshblazer/sluice:0.4.0` and `:latest`
- the Helm chart, versioned to match, attached to a [GitHub Release](https://github.com/JoshBlazer/Sluice/releases) with generated notes

Each image contains `/sluice`, `/sluice-cli`, the `migrate` CLI and the matching migrations in `/migrations`. `sluice --version` prints the build.

## Docker image

```bash
# apply the schema that ships with the image
docker run --rm --entrypoint /migrate ghcr.io/joshblazer/sluice:latest \
  -path /migrations -database "$SLUICE_POSTGRES_URL" up

docker run --rm -e SLUICE_POSTGRES_URL=... -e SLUICE_REDIS_ADDR=... \
  ghcr.io/joshblazer/sluice:latest --role worker
```

`make docker-build` builds the same image locally as `sluice:dev`. Settings are listed in [Configuration](configuration.md).

## Kubernetes (Helm)

The chart expects Postgres, Redis and etcd to exist already. It applies database migrations itself, as a pre-install/pre-upgrade hook job using the image being deployed (set `migrations.enabled=false` to manage the schema yourself).

```bash
helm install sluice deploy/helm \
  --set postgres.url="postgres://sluice:$PG_PASSWORD@postgres:5432/sluice?sslmode=require" \
  --set redis.addr="rediss://:$REDIS_PASSWORD@redis:6380/0" \
  --set worker.replicas=10
```

- **Secrets.** Connection strings and passwords go into a Kubernetes Secret, never the ConfigMap. To use a Secret you manage yourself, set `existingSecret` to its name (keys `SLUICE_POSTGRES_URL`, `SLUICE_REDIS_ADDR`, optionally `SLUICE_ETCD_PASSWORD`).
- **etcd TLS.** Set `etcd.tls.secretName` to a Secret holding `ca.crt` (plus `tls.crt`/`tls.key` with `etcd.tls.clientCert=true`); it is mounted into scheduler pods.
- **Probes.** The API has readiness (`/readyz`, which checks Postgres and Redis) and liveness probes; schedulers and workers have liveness probes on their metrics port.
- **Autoscaling.** Workers scale with a KEDA `ScaledObject` on `sum(sluice_queue_depth)`, which the scheduler leader exports. It needs KEDA installed and a Prometheus that scrapes the scheduler (`worker.autoscaling.prometheusAddress`); set `worker.autoscaling.enabled=false` otherwise.
- **Monitoring.** `monitoring.*` values install the [alert rules](../deploy/helm/files/alerts.yml) as a `PrometheusRule` plus a `PodMonitor`. See [Operations](operations.md#observability).

CI installs the chart on a throwaway [kind](https://kind.sigs.k8s.io/) cluster and runs a job through it on every push ([`scripts/k8s-smoke.sh`](../scripts/k8s-smoke.sh), with test dependencies in [`deploy/kind/deps.yaml`](../deploy/kind/deps.yaml)). Plain manifests without Helm are in [`deploy/k8s/`](../deploy/k8s).

## Recommended production layout

- 2× API replicas (stateless, load-balanced)
- 3× scheduler replicas (1 leader + 2 hot standbys via etcd lease)
- 10× worker replicas (scaled by KEDA on queue depth)
- 1× Postgres primary + 1 replica
- 1× Redis with persistence + 1 replica
- 3× etcd nodes

Postgres is the source of truth, so standard Postgres backup tooling covers everything. Redis needs no backup; its contents are rebuilt from Postgres.
