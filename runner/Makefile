.PHONY: all bench-all bench-mongo bench-cassandra bench-postgres bench-mysql help

help:
	@echo "Database Benchmark Targets:"
	@echo "  make bench-all        - Run benchmarks for all databases"
	@echo "  make bench-mongo      - Run benchmarks for MongoDB only"
	@echo "  make bench-cassandra  - Run benchmarks for Cassandra only"
	@echo "  make bench-postgres   - Run benchmarks for PostgreSQL only"
	@echo "  make bench-mysql      - Run benchmarks for MySQL only"

bench-all:
	@echo "Running benchmarks for all databases..."
	./run_benchmarks.sh

bench-mongo:
	@echo "Running benchmarks for MongoDB..."
	@python3 -c "import yaml; config = yaml.safe_load(open('bench_config_multi.yml')); yaml.dump(config['databases']['mongo'], open('bench_config.yml', 'w'))"
	python3 bench.py

bench-cassandra:
	@echo "Running benchmarks for Cassandra..."
	@python3 -c "import yaml; config = yaml.safe_load(open('bench_config_multi.yml')); yaml.dump(config['databases']['cassandra'], open('bench_config.yml', 'w'))"
	python3 bench.py

bench-postgres:
	@echo "Running benchmarks for PostgreSQL..."
	@python3 -c "import yaml; config = yaml.safe_load(open('bench_config_multi.yml')); yaml.dump(config['databases']['postgres'], open('bench_config.yml', 'w'))"
	python3 bench.py

bench-mysql:
	@echo "Running benchmarks for MySQL..."
	@python3 -c "import yaml; config = yaml.safe_load(open('bench_config_multi.yml')); yaml.dump(config['databases']['mysql'], open('bench_config.yml', 'w'))"
	python3 bench.py
