#!/usr/bin/env bash
# Benchmarks Sluice across real machines, for the README's 3-node performance
# targets: one database host (Postgres, Redis, etcd), N Sluice nodes that each run
# api + scheduler + worker, and a separate load generator so it doesn't compete
# with Sluice for CPU. See docs/benchmarking.md.
#
#   scripts/bench/run.sh hosts.txt
#
# hosts.txt has one host per line: <role> <ssh target> <private IP>
#   db    root@203.0.113.10  10.0.0.2
#   node  root@203.0.113.11  10.0.0.3
#   node  root@203.0.113.12  10.0.0.4
#   node  root@203.0.113.13  10.0.0.5
#   load  root@203.0.113.14  10.0.0.6
# db and node hosts need Docker; every host needs curl and vmstat. Hosts talk to
# each other on the private IPs; Postgres, Redis and etcd listen only there.
set -euo pipefail
cd "$(dirname "$0")/../.."

HOSTS_FILE=${1:?usage: scripts/bench/run.sh hosts.txt}
IMAGE=${IMAGE:-ghcr.io/joshblazer/sluice:0.4.0}
JOBS=${JOBS:-100000}                  # jobs per measured run
SUBMITTERS=${SUBMITTERS:-"64 128 256"} # one measured run per value
CONCURRENCY=${CONCURRENCY:-64}        # jobs each worker runs at once
API_POOL=${API_POOL:-32}              # Postgres connections per API
PG_SHARED_BUFFERS=${PG_SHARED_BUFFERS:-4GB}
LOAD_ARCH=${LOAD_ARCH:-amd64}
CLEANUP=${CLEANUP:-1}                 # 0 leaves every container running afterwards
read -r -a SSH_OPTS <<< "${SSH_OPTS:--o StrictHostKeyChecking=accept-new -o BatchMode=yes -o ConnectTimeout=10}"

DB_SSH='' DB_IP='' LOAD_SSH='' LOAD_IP=''
NODE_SSH=() NODE_IP=()
while read -r role target ip _; do
  case $role in
    ''|\#*) continue ;;
    db)   DB_SSH=$target DB_IP=$ip ;;
    node) NODE_SSH+=("$target"); NODE_IP+=("$ip") ;;
    load) LOAD_SSH=$target LOAD_IP=$ip ;;
    *) echo "unknown role '$role' in $HOSTS_FILE" >&2; exit 2 ;;
  esac
done < <(tr -d '\r' < "$HOSTS_FILE")
[ -n "$DB_SSH" ] && [ -n "$LOAD_SSH" ] && [ ${#NODE_SSH[@]} -gt 0 ] ||
  { echo "$HOSTS_FILE needs a db, a load and at least one node host" >&2; exit 2; }

on() { local h=$1; shift; ssh "${SSH_OPTS[@]}" "$h" "$@"; }
log() { echo "[$(date +%H:%M:%S)] $*"; }
secret() { head -c 16 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

OUT=bench-results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"
PG_PASS=$(secret)
REDIS_PASS=$(secret)
NODES=${#NODE_SSH[@]}
# api + worker (concurrency + 4) + scheduler + sluice-cli, per node, plus headroom
PG_MAX_CONN=$(( NODES * (API_POOL + CONCURRENCY + 4 + 8 + 4) + 20 ))
PG_URL="postgres://sluice:$PG_PASS@$DB_IP:5432/sluice?sslmode=disable"
REDIS_URL="redis://:$REDIS_PASS@$DB_IP:6379/0"
APIS=$(printf "http://%s:8080," "${NODE_IP[@]}"); APIS=${APIS%,}

cat > "$OUT/config.txt" <<EOF
image=$IMAGE nodes=$NODES jobs=$JOBS submitters=($SUBMITTERS)
worker concurrency=$CONCURRENCY api pool_max_conns=$API_POOL
postgres max_connections=$PG_MAX_CONN shared_buffers=$PG_SHARED_BUFFERS synchronous_commit=on (default)
EOF

log "hardware"
for h in "$DB_SSH" "${NODE_SSH[@]}" "$LOAD_SSH"; do
  echo "$h: $(on "$h" 'echo "$(nproc) vCPU, $(free -g | awk "/Mem:/{print \$2}") GB RAM, $(lscpu | sed -n "s/^Model name: *//p")"')"
done | tee "$OUT/hardware.txt"

sluice_down() {
  for h in "${NODE_SSH[@]}"; do on "$h" 'docker rm -f sluice-api sluice-scheduler sluice-worker >/dev/null 2>&1 || true' & done; wait
}
cleanup() {
  for h in "$DB_SSH" "${NODE_SSH[@]}" "$LOAD_SSH"; do on "$h" "pkill vmstat; pkill -f '[s]luice-loadtest'" >/dev/null 2>&1 || true; done
  [ "$CLEANUP" = 1 ] || { log "CLEANUP=0: containers left running"; return; }
  log "stopping containers"
  sluice_down
  on "$DB_SSH" 'docker rm -f -v sluice-pg sluice-redis sluice-etcd >/dev/null 2>&1 || true'
}
trap cleanup EXIT

log "database host: postgres 16, redis 7, etcd"
on "$DB_SSH" "docker rm -f -v sluice-pg sluice-redis sluice-etcd >/dev/null 2>&1 || true
  docker run -d --name sluice-pg --network host -e POSTGRES_USER=sluice -e POSTGRES_PASSWORD=$PG_PASS -e POSTGRES_DB=sluice \
    postgres:16 -c listen_addresses=$DB_IP -c max_connections=$PG_MAX_CONN -c shared_buffers=$PG_SHARED_BUFFERS >/dev/null
  docker run -d --name sluice-redis --network host redis:7 --bind $DB_IP --requirepass $REDIS_PASS >/dev/null
  docker run -d --name sluice-etcd --network host quay.io/coreos/etcd:v3.5.16 etcd --data-dir=/etcd-data \
    --listen-client-urls=http://$DB_IP:2379 --advertise-client-urls=http://$DB_IP:2379 \
    --listen-peer-urls=http://127.0.0.1:2380 --initial-advertise-peer-urls=http://127.0.0.1:2380 \
    --initial-cluster=default=http://127.0.0.1:2380 >/dev/null
  for i in \$(seq 1 60); do docker exec sluice-pg pg_isready -q -h $DB_IP -U sluice && exit 0; sleep 1; done
  echo 'postgres never became ready' >&2; exit 1"

log "pulling $IMAGE on $NODES nodes"
for h in "${NODE_SSH[@]}"; do on "$h" "docker pull -q $IMAGE >/dev/null" & done; wait
sluice_down

log "migrating"
on "${NODE_SSH[0]}" "docker run --rm --network host --entrypoint /migrate $IMAGE -path /migrations -database '$PG_URL' up"
KEY=$(on "${NODE_SSH[0]}" "docker run --rm --network host -e SLUICE_POSTGRES_URL='$PG_URL' -e SLUICE_REDIS_ADDR='$REDIS_URL' \
  --entrypoint /sluice-cli $IMAGE create-tenant -rate-limit 0 bench" | awk '/api key/{print $3}')
[ -n "$KEY" ] || { echo "could not create the bench tenant" >&2; exit 1; }

log "starting api + scheduler + worker on every node"
for h in "${NODE_SSH[@]}"; do
  on "$h" "common='--network host -e SLUICE_REDIS_ADDR=$REDIS_URL -e SLUICE_ETCD_ENDPOINTS=$DB_IP:2379'
    docker run -d --name sluice-api \$common -e 'SLUICE_POSTGRES_URL=$PG_URL&pool_max_conns=$API_POOL' $IMAGE --role api >/dev/null
    docker run -d --name sluice-scheduler \$common -e 'SLUICE_POSTGRES_URL=$PG_URL' $IMAGE --role scheduler >/dev/null
    docker run -d --name sluice-worker \$common -e 'SLUICE_POSTGRES_URL=$PG_URL' -e SLUICE_WEBHOOK_ALLOW_PRIVATE=true \
      $IMAGE --role worker --concurrency $CONCURRENCY >/dev/null" &
done
wait
for ip in "${NODE_IP[@]}"; do
  on "$LOAD_SSH" "for i in \$(seq 1 60); do curl -sf http://$ip:8080/readyz >/dev/null && exit 0; sleep 1; done
    echo 'API on $ip never became ready' >&2; exit 1"
done

log "shipping loadtest ($LOAD_ARCH) to the load host"
GOOS=linux GOARCH=$LOAD_ARCH CGO_ENABLED=0 go build -o "$OUT/loadtest" ./scripts/loadtest
scp -q "${SSH_OPTS[@]}" "$OUT/loadtest" "$LOAD_SSH:/tmp/sluice-loadtest"
on "$LOAD_SSH" 'chmod +x /tmp/sluice-loadtest' # scp from Windows drops the executable bit
rm "$OUT/loadtest"

for h in "$DB_SSH" "${NODE_SSH[@]}" "$LOAD_SSH"; do
  on "$h" 'pkill vmstat; nohup vmstat -n 5 > /tmp/sluice-vmstat.log 2>&1 < /dev/null &' || true
done

loadtest() { # $1 jobs, $2 submitters
  on "$LOAD_SSH" "/tmp/sluice-loadtest -api $APIS -key $KEY -n $1 -c $2 -listen $LOAD_IP:9099 -wait 600s" 2>&1
}
log "warm-up (5,000 jobs, not recorded)"
loadtest 5000 64 > "$OUT/warmup.txt" || true
grep -q 'jobs executed' "$OUT/warmup.txt" || { cat "$OUT/warmup.txt"; echo "warm-up produced no results" >&2; exit 1; }

SUMMARY="| Submitters | Submission (jobs/s) | Submit p99 | Execution (jobs/s) | Submit→execute p50 / p99 | Executed | Duplicates |
|---|---|---|---|---|---|---|"
for c in $SUBMITTERS; do
  log "run: $JOBS jobs, $c submitters"
  loadtest "$JOBS" "$c" | tee "$OUT/run-c$c.txt" || true
  f="$OUT/run-c$c.txt"
  SUMMARY+="
| $c | $(awk '/submission throughput/{print $3}' "$f") | $(awk '/submit latency/{print $6}' "$f") | $(awk '/execution throughput/{print $3}' "$f") | $(awk '/submit→execute/{print $3" / "$5}' "$f") | $(awk '/jobs executed/{print $3}' "$f") | $(awk '/duplicate deliveries/{print $3}' "$f") |"
done

log "collecting CPU usage and logs"
for h in "$DB_SSH" "${NODE_SSH[@]}" "$LOAD_SSH"; do
  name=$(echo "$h" | tr '@:' '__')
  on "$h" 'pkill vmstat; cat /tmp/sluice-vmstat.log' > "$OUT/vmstat-$name.log" 2>/dev/null || true
done
for i in "${!NODE_SSH[@]}"; do
  for r in api scheduler worker; do
    on "${NODE_SSH[$i]}" "docker logs --tail 500 sluice-$r" > "$OUT/node$((i + 1))-$r.log" 2>&1 || true
  done
done

{
  echo "# Sluice benchmark, $(date -u +%Y-%m-%dT%H:%MZ)"
  echo
  echo '```'; cat "$OUT/config.txt"; echo; cat "$OUT/hardware.txt"; echo '```'
  echo
  echo "$SUMMARY"
  echo
  echo "CPU busy (100 - idle) per host from vmstat, average and peak 5-second sample. A host near 100% at peak is the bottleneck:"
  echo
  for f in "$OUT"/vmstat-*.log; do
    awk -v h="$(basename "$f" .log | sed 's/^vmstat-//')" \
      'NR > 2 && $15 ~ /^[0-9]+$/ { b = 100 - $15; s += b; n++; if (b > p) p = b } END { if (n) printf "- %s: %.0f%% average, %d%% peak\n", h, s / n, p }' "$f"
  done
} > "$OUT/summary.md"
echo
cat "$OUT/summary.md"
log "results in $OUT"
