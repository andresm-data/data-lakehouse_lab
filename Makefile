SHELL := /bin/bash

.PHONY: install test test-spark test-report bench-formats dashboard pipeline docker-up docker-test

# Instala dependencias y comandos del proyecto
install:
	poetry install

# Todas las pruebas, con cobertura
test:
	poetry run pytest

# Solo las pruebas de Spark y de versiones
test-spark:
	poetry run pytest tests/spark tests/test_versions.py --no-cov

# Todas las pruebas, guardando la salida en docs/tests/ como evidencia
test-report:
	mkdir -p docs/tests
	poetry run pytest -p no:cacheprovider 2>&1 | tee docs/tests/pytest_$$(date +%Y%m%d_%H%M%S).txt; exit $${PIPESTATUS[0]}

# Mismas consultas sobre Parquet, Delta e Iceberg con DuckDB -> docs/benchmarks/
bench-formats:
	poetry run lh-bench-formats -r 10 --calentamiento 2

# Dashboard de Streamlit en http://localhost:8501
dashboard:
	poetry run streamlit run src/lakehouse/dashboard/app.py

# Proyecto completo: descarga, ingesta, Delta, Iceberg, Spark y benchmarks
pipeline:
	poetry run bash scripts/pipeline.sh

# Construye la imagen y ejecuta el pipeline dentro de Docker
docker-up:
	docker compose up --build

# Pruebas dentro del contenedor
docker-test:
	docker compose run --build --rm tests
