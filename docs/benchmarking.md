# Benchmarking on target hardware

The README's performance targets are for a 3-node cluster of 4 vCPU / 8 GB machines. The [Benchmark workflow](../.github/workflows/benchmark.yml) can only run everything on one shared CI runner. [`scripts/bench`](../scripts/bench) measures the real layout:

| Host | Runs | Default size (Hetzner) |
|------|------|------------------------|
| 3 × node | `api`, `scheduler` and `worker` each, from the release image | `cpx31`: 4 vCPU / 8 GB, the target node |
| db | Postgres 16, Redis 7, etcd | `ccx33`: 8 dedicated vCPU / 32 GB |
| load | the [load generator](../scripts/loadtest) and the webhook endpoint the jobs call | `cpx41`: 8 vCPU / 16 GB |

The database and load hosts are deliberately bigger than the nodes, so the result measures Sluice rather than an undersized Postgres or a saturated load generator. The summary reports CPU use per host, so you can check which one was the bottleneck.

## Running it on Hetzner Cloud

You need the [`hcloud` CLI](https://github.com/hetznercloud/cli), a Hetzner Cloud project, and an SSH key uploaded to that project.

```bash
hcloud context create sluice-bench                     # paste a read/write API token
SSH_KEY=<key name> scripts/bench/hcloud.sh up          # 5 machines, writes scripts/bench/hosts.txt
scripts/bench/run.sh scripts/bench/hosts.txt           # ~15-30 minutes
scripts/bench/hcloud.sh down                           # stop paying
```

A full run keeps 5 machines up for well under an hour, which costs roughly €1 at Hetzner's hourly prices. Check the current prices and server type names with `hcloud server-type list`, and override the defaults with `NODE_TYPE`, `DB_TYPE`, `LOAD_TYPE`, `LOCATION` and `NODES`.

## Running it anywhere else

`run.sh` only needs SSH access to Linux machines with Docker (db and nodes) plus `curl` and `vmstat` on every host, and a private network between them. Write `hosts.txt` yourself:

```
# role  ssh target          private IP
db      ubuntu@203.0.113.10 10.0.0.2
node    ubuntu@203.0.113.11 10.0.0.3
node    ubuntu@203.0.113.12 10.0.0.4
node    ubuntu@203.0.113.13 10.0.0.5
load    ubuntu@203.0.113.14 10.0.0.6
```

Postgres, Redis and etcd get random passwords (etcd has none) and listen only on the db host's private IP. Still, don't leave the machines open to the internet beyond SSH.

To check the script works before paying for machines, `scripts/bench/local.sh up` starts Docker-in-Docker containers that stand in for the hosts. Its numbers mean nothing, because every "host" shares your machine.

## Settings

| Variable | Default | |
|----------|---------|---|
| `IMAGE` | `ghcr.io/joshblazer/sluice:0.4.0` | Sluice image the nodes run |
| `JOBS` | `100000` | Jobs per measured run |
| `SUBMITTERS` | `64 128 256` | One measured run per value, after a 5,000-job warm-up |
| `CONCURRENCY` | `64` | Jobs each worker runs at once |
| `API_POOL` | `32` | Postgres connections per API (`pool_max_conns`) |
| `PG_SHARED_BUFFERS` | `4GB` | |
| `CLEANUP` | `1` | `0` leaves every container running for inspection |

Postgres keeps its default durability (`synchronous_commit=on`), so every accepted job is flushed to disk before the API answers, just as in production. Traces are generated but not exported, because there is no collector.

## Results

Each run writes `bench-results/<timestamp>/`:
- `summary.md`: the throughput and latency table and CPU use per host
- `hardware.txt` and `config.txt`
- the raw loadtest output for each run
- `vmstat` samples from every host
- the last lines of each Sluice process's log

The load generator records both the submit time and the webhook arrival time with its own clock, so submit→execute latency isn't affected by clock skew between machines.
