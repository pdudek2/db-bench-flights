# Database Benchmark Suite

This directory contains the benchmark configuration and runner for testing MongoDB, Cassandra, PostgreSQL, and MySQL with identical scenarios.

## Overview

The benchmark suite is designed to:
- Execute the same test scenarios across all 4 databases
- Avoid code duplication using YAML anchors and shared configuration
- Provide a simple way to run benchmarks for individual or all databases
- Record results in a CSV file for analysis

## Files

- `bench_config_multi.yml` - DRY configuration file using YAML anchors for shared scenario across all databases
- `bench_config.yml` - Single database configuration (generated automatically or manually created)
- `bench.py` - Main benchmark runner with implementations for all 4 databases
- `run_benchmarks.sh` - Shell script to run benchmarks for all databases sequentially
- `results/results.csv` - Output file with benchmark results

## Configuration Structure

### bench_config_multi.yml

Uses YAML anchors to define a common scenario once and reuse it across all databases:

```yaml
common_scenario: &common_scenario
  dataset_name: "100k"
  repeats: 5
  queries: { ... }
  crud: { ... }

databases:
  mongo:
    <<: *common_scenario
    db: mongo
    connection: { ... }
  
  cassandra:
    <<: *common_scenario
    db: cassandra
    connection: { ... }
  
  postgres:
    <<: *common_scenario
    db: postgres
    connection: { ... }
  
  mysql:
    <<: *common_scenario
    db: mysql
    connection: { ... }
```

## Benchmark Scenarios

Each database implements the same scenarios:

### Read Operations
1. **read_by_carrier_day** - Query flights by carrier and date range
2. **top_routes_month** - Find top 10 routes by delay in a month
3. **histogram_arr_delay** - Generate histogram of arrival delays

### Write Operations
4. **insert_batch** - Batch insert of sample flights
5. **update_many** - Update multiple records
6. **delete_many** - Delete multiple records

## Running Benchmarks

### Option 1: Run All Databases (Recommended)

```bash
cd /app
./run_benchmarks.sh
```

This will:
1. Run benchmarks for MongoDB
2. Run benchmarks for Cassandra
3. Run benchmarks for PostgreSQL
4. Run benchmarks for MySQL
5. Append all results to `results/results.csv`

### Option 2: Run Single Database

Extract a single database config from the multi-config:

```bash
# Using Python
python3 << 'EOF'
import yaml
with open('bench_config_multi.yml', 'r') as f:
    config = yaml.safe_load(f)
db_config = config['databases']['postgres']
with open('bench_config.yml', 'w') as f:
    yaml.dump(db_config, f)
EOF

# Then run
python3 bench.py
```

Or use the original single-db config format:

```bash
# Edit bench_config.yml to set db: postgres
python3 bench.py
```

### Option 3: Run via Docker Compose

From the project root:

```bash
docker compose exec runner bash
cd /app
pip install -r requirements.txt
./run_benchmarks.sh
```

## Environment Variables

The benchmark runner supports environment variables for connection settings:

**MongoDB:**
- `MONGO_HOST` (default: mongodb)

**Cassandra:**
- (uses hardcoded host: cassandra)

**PostgreSQL:**
- `POSTGRES_HOST` (default: postgres)
- `POSTGRES_PORT` (default: 5432)
- `POSTGRES_DB` (default: flights)
- `POSTGRES_USER` (default: flights_user)
- `POSTGRES_PASSWORD` (default: flights_pass)

**MySQL:**
- `MYSQL_HOST` (default: mysql)
- `MYSQL_PORT` (default: 3306)
- `MYSQL_DATABASE` (default: flights)
- `MYSQL_USER` (default: flights_user)
- `MYSQL_PASSWORD` (default: flights_pass)

## Results

Results are logged to `results/results.csv` with the following columns:

- `ts` - Timestamp of the test
- `db` - Database name (mongo, cassandra, postgres, mysql)
- `dataset` - Dataset name (e.g., "100k")
- `scenario` - Scenario name (e.g., "pg_read_by_carrier_day")
- `repeat` - Repeat number (1 to N)
- `elapsed_ms` - Execution time in milliseconds
- `notes` - Additional information (e.g., "found=1000")

## Customizing Scenarios

To modify test parameters, edit `bench_config_multi.yml`:

```yaml
common_scenario: &common_scenario
  dataset_name: "1M"          # Change dataset identifier
  repeats: 10                  # Change number of repeats
  
  queries:
    read_by_carrier_day:
      carrier: "AA"            # Change carrier code
      date_from: "2024-01-01"  # Change date range
      date_to: "2024-01-31"
      limit: 5000              # Change result limit
```

The changes will automatically apply to all databases.

## Troubleshooting

**Issue: "Unknown db in bench_config.yml"**
- Ensure `db` field is set to one of: mongo, cassandra, postgres, mysql

**Issue: Connection errors**
- Verify database containers are running: `docker compose ps`
- Check database health: `docker compose exec <db> <health-check-command>`
- Verify environment variables match your setup

**Issue: Missing Python dependencies**
- Run: `pip install -r requirements.txt`

**Issue: yq not found (when running run_benchmarks.sh)**
- The script will automatically fall back to Python for YAML processing
- Or install yq: https://github.com/mikefarah/yq

## Implementation Notes

### Why separate implementations per database?

Each database has different:
- Connection methods and drivers
- Query syntax (SQL vs NoSQL)
- Data models and schema structure
- Performance characteristics

The benchmark ensures **logically equivalent** operations across all databases, even if the implementation differs.

### Cassandra Keyspace Note

The current implementation uses keyspace "flightsks" but the schema creates "flights". 
You may need to adjust `cass_client()` in `bench.py` or update the schema file to match.

## Related Files

See also:
- `/scenario.txt` - Original scenario requirements in Polish
- `/docker-compose.yml` - Database container definitions
- `/docker/*/init/` - Database initialization scripts
