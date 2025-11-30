#!/bin/bash
set -euo pipefail

echo "========================================="
echo ">>> Running benchmarks"
echo "========================================="

echo "1/4 docker compose up (databases + runner)"
docker compose up -d

echo "2/4 initializing Cassandra schema"
docker exec dbbench-cassandra cqlsh -f /docker-entrypoint-initdb.d/schema.cql

echo "3/4 installing Python dependencies in runner"
docker compose exec -T runner bash -lc "cd /app && pip install -r requirements.txt"

echo "4/4 running bench_runner.py"
docker compose exec -T runner bash -lc "cd /app && python bench_runner.py"

echo "stopping containers and removing volumes (docker compose down -v)"
docker compose down -v


echo "========================================="
echo ">>> Running analyze_results.py and plot_results.py on accumulated results"
echo "========================================="

docker compose up -d runner
docker compose exec -T runner bash -lc "cd /app && pip install -r requirements.txt && python analyze_results.py && python plot_results.py"
docker compose down -v

echo "done. Results are in runner/results/results.csv and runner/results/charts/"