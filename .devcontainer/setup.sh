#!/usr/bin/env bash
# Runs once when the Codespace (or dev container) is created: builds everything
# start.sh runs. Codespaces prebuilds can run this ahead of time. No VCS stamping:
# git refuses repos mounted with a different owner, as in local dev containers.
set -euo pipefail
cd "$(dirname "$0")/.."

go install -tags 'pgx5' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.18.1
go build -buildvcs=false -o bin/sluice ./cmd/sluice
go build -buildvcs=false -o bin/sluice-cli ./cmd/sluice-cli
go build -buildvcs=false -o bin/demo ./scripts/demo
cd web && npm ci && npm run build
