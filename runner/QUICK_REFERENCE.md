# Quick Reference Guide

## Running Benchmarks

### All Databases
```bash
./run_benchmarks.sh
# or
make bench-all
```

### Single Database
```bash
make bench-mongo
make bench-cassandra
make bench-postgres
make bench-mysql
```

## Configuration Files

| File | Purpose |
|------|---------|
| `bench_config_multi.yml` | Main DRY config for all 4 databases |
| `bench_config.yml` | Single database config (generated) |
| `bench_config_examples.yml` | Example configurations for different scenarios |

## Scenario Operations

Each database implements 6 identical scenarios:

| Operation | Description |
|-----------|-------------|
| `read_by_carrier_day` | Query flights by carrier and date range |
| `top_routes_month` | Find top 10 routes by delay in a month |
| `histogram_arr_delay` | Generate histogram of arrival delays |
| `insert_batch` | Batch insert of sample flights |
| `update_many` | Update multiple records |
| `delete_many` | Delete multiple records |

## Customizing Tests

Edit `bench_config_multi.yml` to change:
- `dataset_name`: Label for the dataset being tested
- `repeats`: Number of times to repeat each scenario
- `queries.*.carrier`: Carrier code to query (e.g., "AA", "B6", "DL")
- `queries.*.date_from/date_to`: Date range for queries
- `queries.*.limit`: Maximum results to fetch
- `queries.*.month`: Month for aggregation queries
- `crud.sample_size_for_writes`: Number of records for write operations

## Results

Results are saved to `results/results.csv`:
```csv
ts,db,dataset,scenario,repeat,elapsed_ms,notes
2024-01-15T10:30:00,mongo,100k,mongo_read_by_carrier_day,1,123.45,found=1000
```

## Docker Usage

```bash
# From project root
docker compose up -d
docker compose exec runner bash

# Inside runner container
cd /app
pip install -r requirements.txt
./run_benchmarks.sh
```

## Troubleshooting

**Connection refused errors:**
```bash
# Check if databases are running
docker compose ps

# Restart specific database
docker compose restart postgres
```

**Import error for Python modules:**
```bash
pip install -r requirements.txt
```

**Cassandra keyspace error:**
The code uses "flightsks" but schema creates "flights". Either:
- Update bench.py `cass_client()` to use "flights", or
- Update schema.cql to create "flightsks"
```

## File Structure

```
runner/
├── bench.py                    # Main benchmark runner (updated)
├── bench_config_multi.yml      # DRY config for all databases
├── bench_config.yml            # Single DB config (generated)
├── bench_config_examples.yml   # Example configurations
├── run_benchmarks.sh           # Shell script to run all
├── Makefile                    # Make targets
├── README_BENCHMARKS.md        # Full documentation
├── QUICK_REFERENCE.md          # This file
└── results/
    └── results.csv             # Benchmark results
```
