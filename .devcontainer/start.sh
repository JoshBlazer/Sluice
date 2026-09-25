#!/usr/bin/env bash
# Runs every time the Codespace starts: dependencies in Docker, then each Sluice
# role, demo traffic and the dashboard in the background. Logs go to /tmp/sluice.
set -euo pipefail
cd "$(dirname "$0")/.."
LOGS=/tmp/sluice
mkdir -p "$LOGS"

for _ in $(seq 1 60); do docker info > /dev/null 2>&1 && break; sleep 1; done
docker compose up -d --wait postgres redis etcd jaeger prometheus
migrate -path migrations -database "pgx5://sluice:sluice@localhost:5433/sluice?sslmode=disable" up
./bin/sluice-cli enable-dev-tenant

pkill -f '[b]in/sluice --role' || true
pkill -f '[b]in/demo' || true
pkill -f '[n]ext start' || true
run() { local name=$1; shift; setsid nohup "$@" > "$LOGS/$name.log" 2>&1 < /dev/null & }

run api ./bin/sluice --role api
run scheduler ./bin/sluice --role scheduler
SLUICE_WEBHOOK_ALLOW_PRIVATE=true run worker ./bin/sluice --role worker
for _ in $(seq 1 60); do curl -sf localhost:8080/readyz > /dev/null && break; sleep 1; done
run demo ./bin/demo -key dev-token
(cd web && run dashboard npm run start -- -p 3000)
for _ in $(seq 1 60); do curl -sf -o /dev/null localhost:3000 && break; sleep 1; done
echo "Sluice is running: dashboard on port 3000, logs in $LOGS"
