#!/usr/bin/env bash
# Failure-mode tests: inject a fault while a load test is in flight and check that
# every job the API accepted still executes. Reports duplicates (at-least-once
# re-runs) and how long after the fault the backlog fully drained.
#
# Expects: Postgres/Redis/etcd from docker-compose (migrated), binaries in ./bin
# (sluice, sluice-cli, loadtest). Linux/macOS. Used by .github/workflows/chaos.yml.
set -uo pipefail
cd "$(dirname "$0")/.."

N=${N:-3000}            # jobs per scenario
FAULT_AT=${FAULT_AT:-5} # seconds into the run
LOG=chaos-logs
mkdir -p "$LOG"
export SLUICE_POSTGRES_URL=${SLUICE_POSTGRES_URL:-postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable}

declare -A PIDS
ROLES=(api sched1 sched2 worker1 worker2)

start() {
  local name=$1 args
  case $name in
    api)     args=(--role api) ;;
    sched1)  args=(--role scheduler --metrics-port 9091) ;;
    sched2)  args=(--role scheduler --metrics-port 9093) ;;
    worker1) args=(--role worker --metrics-port 9201 --concurrency 20) ;;
    worker2) args=(--role worker --metrics-port 9202 --concurrency 20) ;;
  esac
  SLUICE_WEBHOOK_ALLOW_PRIVATE=true ./bin/sluice "${args[@]}" >> "$LOG/$name.log" 2>&1 &
  PIDS[$name]=$!
}
alive() { [ -n "${PIDS[$1]:-}" ] && kill -0 "${PIDS[$1]}" 2>/dev/null; }
kill9() { kill -9 "${PIDS[$1]}" 2>/dev/null; wait "${PIDS[$1]}" 2>/dev/null; unset "PIDS[$1]"; }
ensure_all() {
  for r in "${ROLES[@]}"; do alive "$r" || start "$r"; done
  for _ in $(seq 1 60); do curl -sf localhost:8080/readyz >/dev/null && return; sleep 0.5; done
  echo "API never became ready"; exit 1
}
leader() {
  for s in sched1:9091 sched2:9093; do
    curl -s "localhost:${s#*:}/metrics" | grep -q '^sluice_scheduler_is_leader 1' && { echo "${s%%:*}"; return; }
  done
}

# --- faults ------------------------------------------------------------------
fault_worker_kill()     { kill9 worker1; sleep 5; start worker1; }
fault_leader_kill()     { local l; l=$(leader); echo "killing leader $l"; kill9 "$l"; sleep 10; start "$l"; }
fault_redis_flush()     { docker compose exec -T redis redis-cli FLUSHALL >/dev/null; }
fault_redis_restart()   { docker compose restart redis >/dev/null 2>&1; }
fault_postgres_restart(){ docker compose restart postgres >/dev/null 2>&1; }
fault_node_loss()       { for r in "${ROLES[@]}"; do kill9 "$r"; done; sleep 2; ensure_all; }

# --- runner ------------------------------------------------------------------
ensure_all
KEY=$(./bin/sluice-cli create-tenant -rate-limit 0 chaos | awk '/api key/{print $3}')
[ -n "$KEY" ] || { echo "could not create tenant"; exit 1; }

SUMMARY="| Scenario | Accepted | Executed | Duplicates | Drained after fault | Result |
|---|---|---|---|---|---|"
FAILED=0
PORT=9300
for s in worker_kill leader_kill redis_flush redis_restart postgres_restart node_loss; do
  echo "::group::$s"
  ensure_all
  PORT=$((PORT + 1))
  ./bin/loadtest -key "$KEY" -n "$N" -c 16 -delay 200ms -wait 240s \
    -listen "127.0.0.1:$PORT" > "$LOG/$s.txt" 2>&1 &
  lt=$!
  sleep "$FAULT_AT"
  t0=$(date +%s.%N)
  "fault_$s"
  wait "$lt"
  t1=$(date +%s.%N)
  cat "$LOG/$s.txt"

  accepted=$(awk '/jobs submitted/{print $3}' "$LOG/$s.txt")
  executed=$(awk '/jobs executed/{split($3,a,"/"); print a[1]}' "$LOG/$s.txt")
  dupes=$(awk '/duplicate deliveries/{print $3}' "$LOG/$s.txt")
  drained=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1fs", b-a}')
  if [ -n "$accepted" ] && [ "$accepted" = "$executed" ]; then result=pass; else result=FAIL; FAILED=1; fi
  SUMMARY+="
| $s | ${accepted:-?} | ${executed:-?} | ${dupes:-?} | $drained | $result |"
  echo "$s: accepted=$accepted executed=$executed duplicates=$dupes drained=$drained -> $result"
  echo "::endgroup::"
done

for r in "${ROLES[@]}"; do alive "$r" && kill9 "$r"; done
echo "$SUMMARY"
[ -n "${GITHUB_STEP_SUMMARY:-}" ] && { echo "## Chaos tests"; echo; echo "$SUMMARY"; } >> "$GITHUB_STEP_SUMMARY"
exit $FAILED
