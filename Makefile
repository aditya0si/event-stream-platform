# event-stream-platform — developer entry points.
#
# `make` is not required: every target here is one or two plain commands, and the README
# lists them for hosts without it (this project's author develops on Windows without make
# on PATH, so the raw commands are the ones that get exercised).
#
# The one deliberate exception is `test-with-db`: it sets the environment the suite needs
# and runs it, because a test run without those variables fails loudly by design.

SHELL := /bin/bash

PG_HOST      ?= localhost
PG_PORT      ?= 5433
PG_USER      ?= esp
PG_PASS      ?= esp
DB           ?= event_stream
TEST_DB      ?= event_stream_test

PG_URL          ?= postgres://$(PG_USER):$(PG_PASS)@$(PG_HOST):$(PG_PORT)/$(DB)?sslmode=disable
TEST_URL        ?= postgres://$(PG_USER):$(PG_PASS)@$(PG_HOST):$(PG_PORT)/$(TEST_DB)?sslmode=disable
REDIS_URL       ?= redis://localhost:6380/0
TEST_REDIS_URL  ?= redis://localhost:6380/1
BROKER_SEEDS    ?= localhost:19092

.PHONY: help up down logs ps demo migrate status topics seed build test test-with-db fmt vet gate testdb-create testdb-drop smoke load-ingest load-e2e load-sse

help:
	@echo "event-stream-platform"
	@echo
	@echo "  make up              build and start the stack (postgres, redis, redpanda, migrate, ingest)"
	@echo "  make down            stop the stack and delete its volumes"
	@echo "  make ps              show container status"
	@echo "  make logs            follow the ingest logs"
	@echo "  make migrate         apply migrations and ensure broker topics (host-run binary)"
	@echo "  make status          report applied and pending migrations"
	@echo "  make topics          ensure broker topics only"
	@echo "  make testdb-create   create the test database"
	@echo "  make test            run the suite against the test database and broker"
	@echo "  make gate            gofmt + vet + build (what CI runs first)"
	@echo "  make smoke           run the end-to-end smoke test against the running stack"
	@echo "  make demo            start the deterministic fleet so the viewer has something to draw"
	@echo "  make load-ingest     k6 ingest benchmark (NFR-1; needs k6 on PATH)"
	@echo "  make load-e2e        end-to-end latency measurement (NFR-2)"
	@echo "  make load-sse        concurrent SSE fan-out measurement (NFR-3; needs the demo up)"
	@echo
	@echo "Ports: postgres $(PG_PORT), redis 6380, redpanda 19092, ingest 8081"
	@echo "DATABASE_URL is required by every binary; every other value has a default."
	@echo "See .env.example for the full list."

# --- stack ---

up:
	docker compose up -d --build
	@echo "waiting for the migration container..."
	@docker compose wait migrate >/dev/null 2>&1 || true
	@docker compose ps

down:
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f ingest

# --- host-run helpers (talk to the compose stack over its published ports) ---

migrate:
	DATABASE_URL="$(PG_URL)" BROKER_SEEDS="$(BROKER_SEEDS)" go run ./cmd/migrate all

status:
	DATABASE_URL="$(PG_URL)" go run ./cmd/migrate status

topics:
	BROKER_SEEDS="$(BROKER_SEEDS)" go run ./cmd/migrate topics

# --- test database ---
#
# The suite runs against a real Postgres, a real Redis, and a real broker. A fake would
# answer none of the questions these tests ask — partition counts, offset behaviour,
# transaction visibility — so a missing dependency is a loud failure rather than a skip.

testdb-create:
	@docker exec esp-postgres psql -U $(PG_USER) -d postgres -tc \
	  "SELECT 1 FROM pg_database WHERE datname='$(TEST_DB)'" | grep -q 1 \
	  || docker exec esp-postgres createdb -U $(PG_USER) -O $(PG_USER) $(TEST_DB)
	DATABASE_URL="$(TEST_URL)" BROKER_SEEDS="$(BROKER_SEEDS)" go run ./cmd/migrate all

testdb-drop:
	@docker exec esp-postgres dropdb -U $(PG_USER) --if-exists $(TEST_DB)

test: testdb-create
	TEST_DATABASE_URL="$(TEST_URL)" \
	TEST_REDIS_URL="$(TEST_REDIS_URL)" \
	TEST_BROKER_SEEDS="$(BROKER_SEEDS)" \
	go test -race -count=1 ./...

# Same as `test`, without the race detector: this host's MinGW toolchain is 32-bit and
# cannot link the race runtime, so contributors on Windows use this and CI runs the race
# build on Linux.
test-with-db:
	TEST_DATABASE_URL="$(TEST_URL)" \
	TEST_REDIS_URL="$(TEST_REDIS_URL)" \
	TEST_BROKER_SEEDS="$(BROKER_SEEDS)" \
	go test -count=1 ./...

# --- quality gates ---

fmt:
	gofmt -l .

vet:
	go vet ./...

build:
	go build ./...

gate: fmt vet build
	@echo "gate passed"

smoke:
	python scripts/smoke_compose.py

# --- the demo and the measurements ---

# The deterministic fleet (ADR-008). Profiled, so the default stack stays quiet: several smoke
# checks assert on drained consumer lag and row counts that do not move, and a producer running
# throughout would quietly turn them into measurements of the simulator.
demo:
	docker compose --profile demo up -d --build simulate
	@echo "the fleet is running; open http://localhost:8082/ for the live map"
	@echo "stop it with: docker compose stop simulate"

# Each of these is the script that produced the artifact the README quotes. They are listed
# separately rather than chained because they interfere: the ingest benchmark at 2,500 events/s
# leaves the consumer behind, and a latency run started immediately afterwards would time that
# backlog instead of the path.
load-ingest:
	k6 run load/ingest.js

load-e2e:
	python scripts/load_e2e.py

load-sse:
	python scripts/load_sse.py
