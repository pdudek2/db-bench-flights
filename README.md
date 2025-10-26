# db-bench-flights

Porównanie wydajności PostgreSQL, MySQL, MongoDB i Cassandry na danych o lotach (CRUD + prosta analityka).

## Start
```
docker compose up -d
docker exec -it dbbench-cassandra cqlsh -f /docker-entrypoint-initdb.d/schema.cql
docker compose exec runner bash
pip install -r requirements.txt
```

## Dane
- Surowy CSV: `data/raw/`
- Próbki: `data/processed/`

## Import
```
docker compose exec mongodb bash -lc 'mongoimport --db flightsdb --collection flights --type csv --headerline --file /imports/processed/flights_100000.csv --numInsertionWorkers 8'
```

## Running Benchmarks

### Quick Start
```bash
docker compose exec runner bash
cd /app
pip install -r requirements.txt
./run_benchmarks.sh
```

This will run the same benchmark scenarios across all 4 databases (MongoDB, Cassandra, PostgreSQL, MySQL).

### Individual Database Benchmarks
```bash
cd /app
make bench-mongo      # MongoDB only
make bench-postgres   # PostgreSQL only
make bench-mysql      # MySQL only
make bench-cassandra  # Cassandra only
```

### Configuration
The benchmarks use a DRY configuration file (`bench_config_multi.yml`) that defines the common scenario once and applies it to all databases. See `runner/README_BENCHMARKS.md` for detailed documentation.

Results are saved to `runner/results/results.csv`.
