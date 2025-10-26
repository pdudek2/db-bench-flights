#!/usr/bin/env bash
# Benchmark Runner for All Databases
# Executes the same benchmark scenarios for mongo, cassandra, postgres, and mysql

set -euo pipefail

CONFIG="bench_config_multi.yml"
DBS=("mongo" "cassandra" "postgres" "mysql")

echo "========================================"
echo "Starting Benchmark Suite for All DBs"
echo "========================================"
echo ""

for DB in "${DBS[@]}"; do
  echo "========================================"
  echo "Running benchmarks for: $DB"
  echo "========================================"
  
  # Extract the database-specific config section and create a temporary config
  # Using yq or Python to extract the section
  if command -v yq &> /dev/null; then
    # Use yq if available (mikefarah/yq)
    yq eval ".databases.$DB" "$CONFIG" > "bench_config.yml"
  else
    # Fallback to Python YAML manipulation
    python3 << EOF
import yaml
with open('$CONFIG', 'r') as f:
    config = yaml.safe_load(f)
db_config = config['databases']['$DB']
with open('bench_config.yml', 'w') as f:
    yaml.dump(db_config, f)
EOF
  fi
  
  # Run the benchmark
  python3 bench.py
  
  echo ""
  echo "Completed benchmarks for: $DB"
  echo ""
done

echo "========================================"
echo "All benchmarks completed!"
echo "Results saved to: results/results.csv"
echo "========================================"
