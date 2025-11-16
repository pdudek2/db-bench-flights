#!/usr/bin/env bash
set -euo pipefail

echo "1/5 docker compose up (databases + runner)"
docker compose up -d

echo "2/5 initializing Cassandra schema"
docker exec dbbench-cassandra cqlsh -f /docker-entrypoint-initdb.d/schema.cql

echo "3/5 installing Python dependencies in runner"
docker compose exec -T runner bash -lc "cd /app && pip install -r requirements.txt"

echo "4/5 running bench_runner.py"
docker compose exec -T runner bash -lc "cd /app && python bench_runner.py"

echo "5/5 running analyze_results.py"
docker compose exec -T runner bash -lc "cd /app && python analyze_results.py"

echo "stopping containers and removing volumes (docker compose down -v)"
docker compose down -v

echo "done. Results are in runner/results/results.csv"
