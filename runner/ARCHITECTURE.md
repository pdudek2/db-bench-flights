# Benchmark Suite Architecture

## Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                    bench_config_multi.yml                        │
│                  (DRY Config with YAML Anchors)                  │
│                                                                   │
│  common_scenario: &common_scenario                               │
│    dataset_name: "100k"                                          │
│    repeats: 5                                                     │
│    queries: {...}  ◄────── Defined once                         │
│    crud: {...}                                                    │
│                                                                   │
│  databases:                                                       │
│    mongo:         <<: *common_scenario  ◄─┐                     │
│    cassandra:     <<: *common_scenario  ◄─┼─ Inherited          │
│    postgres:      <<: *common_scenario  ◄─┤                     │
│    mysql:         <<: *common_scenario  ◄─┘                     │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│                     run_benchmarks.sh                            │
│                   (Sequential Executor)                          │
│                                                                   │
│  for DB in mongo cassandra postgres mysql:                       │
│    - Extract config for DB                                       │
│    - Generate bench_config.yml                                   │
│    - Execute: python3 bench.py                                   │
└──────────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┬──────────────┐
        ▼                   ▼                   ▼              ▼
  ┌──────────┐       ┌──────────┐       ┌──────────┐   ┌──────────┐
  │  MongoDB │       │ Cassandra│       │PostgreSQL│   │  MySQL   │
  │          │       │          │       │          │   │          │
  │ 6 scenes │       │ 4 scenes │       │ 6 scenes │   │ 6 scenes │
  └──────────┘       └──────────┘       └──────────┘   └──────────┘
        │                   │                   │              │
        └───────────────────┴───────────────────┴──────────────┘
                                  │
                                  ▼
                    ┌───────────────────────────┐
                    │   results/results.csv     │
                    │                           │
                    │  ts, db, dataset,         │
                    │  scenario, repeat,        │
                    │  elapsed_ms, notes        │
                    └───────────────────────────┘
```

## Data Flow

### 1. Configuration Inheritance

```
common_scenario (base)
    │
    ├─── mongo config
    │    ├─ dataset_name: "100k"  (inherited)
    │    ├─ repeats: 5            (inherited)
    │    ├─ queries: {...}        (inherited)
    │    ├─ crud: {...}           (inherited)
    │    └─ connection: {...}     (specific)
    │
    ├─── postgres config
    │    ├─ dataset_name: "100k"  (inherited)
    │    ├─ repeats: 5            (inherited)
    │    ├─ queries: {...}        (inherited)
    │    ├─ crud: {...}           (inherited)
    │    └─ connection: {...}     (specific)
    │
    └─── (similar for cassandra and mysql)
```

### 2. Execution Flow

```
1. User runs: ./run_benchmarks.sh
         │
         ▼
2. For each DB:
   ├─ Load bench_config_multi.yml
   ├─ Extract databases.<db> section
   ├─ Write to bench_config.yml
   └─ Execute python3 bench.py
         │
         ▼
3. bench.py:
   ├─ Load bench_config.yml
   ├─ Read db field
   ├─ Route to appropriate runner:
   │  ├─ run_mongo()
   │  ├─ run_cassandra()
   │  ├─ run_postgres()
   │  └─ run_mysql()
   │
   ├─ For each scenario:
   │  ├─ For each repeat (1..N):
   │  │  ├─ Execute scenario function
   │  │  ├─ Measure elapsed time
   │  │  └─ Log to results.csv
   │  └─ Next repeat
   └─ Next scenario
         │
         ▼
4. Results saved to: results/results.csv
```

## Scenario Mapping

Each database implements logically equivalent operations:

```
┌────────────────────────┬────────────────────────────────────┐
│ Scenario               │ Implementation                     │
├────────────────────────┼────────────────────────────────────┤
│ read_by_carrier_day    │ Query by carrier + date range      │
│                        │   - MongoDB: col.find()            │
│                        │   - Cassandra: SELECT with WHERE   │
│                        │   - PostgreSQL: JOIN + WHERE       │
│                        │   - MySQL: JOIN + WHERE            │
├────────────────────────┼────────────────────────────────────┤
│ top_routes_month       │ Aggregate routes by delay          │
│                        │   - MongoDB: $group + $sort        │
│                        │   - Cassandra: ALLOW FILTERING     │
│                        │   - PostgreSQL: GROUP BY + AVG     │
│                        │   - MySQL: GROUP BY + AVG          │
├────────────────────────┼────────────────────────────────────┤
│ histogram_arr_delay    │ Bucket delays into bins            │
│                        │   - MongoDB: $bucket               │
│                        │   - Cassandra: client-side calc    │
│                        │   - PostgreSQL: CASE WHEN          │
│                        │   - MySQL: CASE WHEN               │
├────────────────────────┼────────────────────────────────────┤
│ insert_batch           │ Bulk insert N records              │
│                        │   - MongoDB: insert_many()         │
│                        │   - PostgreSQL: executemany()      │
│                        │   - MySQL: executemany()           │
├────────────────────────┼────────────────────────────────────┤
│ update_many            │ Update multiple records            │
│                        │   - MongoDB: update_many()         │
│                        │   - PostgreSQL: UPDATE + JOIN      │
│                        │   - MySQL: UPDATE + JOIN           │
├────────────────────────┼────────────────────────────────────┤
│ delete_many            │ Delete multiple records            │
│                        │   - MongoDB: delete_many()         │
│                        │   - PostgreSQL: DELETE WHERE       │
│                        │   - MySQL: DELETE WHERE            │
└────────────────────────┴────────────────────────────────────┘
```

## Configuration Options

### Via bench_config_multi.yml
```yaml
common_scenario:
  dataset_name: "100k"           # Label for dataset
  repeats: 5                     # Number of repetitions
  
  queries:
    read_by_carrier_day:
      carrier: "B6"              # Carrier code
      date_from: "2024-01-01"    # Start date
      date_to: "2024-01-07"      # End date
      limit: 1000                # Max results
    
    top_routes_month:
      month: "2024-01"           # Month to analyze
    
    histogram_arr_delay:
      bins: [...]                # Histogram bin edges
  
  crud:
    sample_size_for_writes: 5000 # Batch size for writes
```

### Via Environment Variables
```bash
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=flights
POSTGRES_USER=flights_user
POSTGRES_PASSWORD=flights_pass

# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=flights
MYSQL_USER=flights_user
MYSQL_PASSWORD=flights_pass

# MongoDB
MONGO_HOST=localhost
```

## File Structure

```
runner/
├── Configuration
│   ├── bench_config_multi.yml      ← Main DRY config
│   ├── bench_config.yml            ← Generated single-DB config
│   └── bench_config_examples.yml   ← Example configs
│
├── Execution
│   ├── bench.py                    ← Main benchmark runner
│   ├── run_benchmarks.sh           ← Sequential executor
│   └── Makefile                    ← Make targets
│
├── Documentation
│   ├── README_BENCHMARKS.md        ← Full documentation
│   ├── QUICK_REFERENCE.md          ← Quick reference
│   ├── IMPLEMENTATION.md           ← Implementation summary
│   └── ARCHITECTURE.md             ← This file
│
└── Results
    └── results/results.csv         ← Benchmark output
```

## Extension Points

To add a new database:

1. Add configuration to `bench_config_multi.yml`:
   ```yaml
   databases:
     newdb:
       <<: *common_scenario
       db: newdb
       connection: {...}
   ```

2. Implement in `bench.py`:
   ```python
   def newdb_client(): ...
   def s_newdb_read_by_carrier_day(cfg): ...
   def s_newdb_top_routes_month(cfg): ...
   # ... other scenarios
   def run_newdb(cfg): ...
   ```

3. Add routing in `bench.py` main block:
   ```python
   elif db == "newdb":
       run_newdb(cfg)
   ```

4. Add to `run_benchmarks.sh`:
   ```bash
   DBS=("mongo" "cassandra" "postgres" "mysql" "newdb")
   ```

## Benefits of This Architecture

1. **DRY Principle**: Configuration defined once, used everywhere
2. **Consistency**: Same parameters for all databases
3. **Maintainability**: Single point of change
4. **Scalability**: Easy to add new databases
5. **Flexibility**: Can override common settings per DB if needed
6. **Automation**: One command runs everything
7. **Traceability**: CSV results with full metadata
