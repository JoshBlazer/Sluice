.PHONY: dev test test-unit test-integration lint migrate-up migrate-down docker-build bootstrap

BINARY=sluice
MODULE=github.com/sluice
POSTGRES_URL?=postgres://sluice:sluice@localhost:5433/sluice?sslmode=disable
MIGRATE=migrate -path migrations -database "pgx5://sluice:sluice@localhost:5433/sluice?sslmode=disable"

bootstrap:
	go install -tags 'pgx5' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
	go mod download

migrate-up:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

migrate-drop:
	$(MIGRATE) drop -f

test-unit:
	go test ./... -count=1

test-integration:
	go test ./... -count=1 -p 1 -tags integration

test: test-unit test-integration

lint:
	go vet ./...

dev-api:
	go run ./cmd/sluice --role api

dev-scheduler:
	go run ./cmd/sluice --role scheduler

dev-worker:
	go run ./cmd/sluice --role worker

docker-build:
	docker build -t sluice:dev .

up:
	docker compose up -d

down:
	docker compose down
