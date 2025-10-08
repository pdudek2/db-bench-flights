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
