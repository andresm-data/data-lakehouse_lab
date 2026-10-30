SHELL := /bin/bash

.PHONY: install test test-spark test-report bench-formats dashboard dashboard-check notebook pipeline demo demo-clean docker-up docker-test docker-dashboard docker-dashboard-check

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

# Comprueba que las 3 pestañas del dashboard cargan con los datos de data/
dashboard-check:
	poetry run python scripts/check_dashboard.py

# Ejecuta el notebook final con los datos de data/ y guarda las salidas
notebook:
	poetry run python scripts/run_notebook.py

# Proyecto completo: descarga, ingesta, Delta, Iceberg, Spark y benchmarks
pipeline:
	poetry run bash scripts/pipeline.sh

# Demo completa en Docker: imagen, pipeline, pruebas, notebook y dashboard en :8501
demo:
	bash scripts/demo.sh

# Borra los contenedores, los volúmenes (datos y jars) y la imagen, para repetir la demo desde cero
demo-clean:
	docker compose --profile dashboard --profile tests down --volumes --rmi all --remove-orphans

# Construye la imagen y ejecuta el pipeline dentro de Docker
docker-up:
	docker compose up --build

# Pruebas dentro del contenedor
docker-test:
	docker compose run --build --rm tests

# Dashboard dentro de Docker en http://localhost:8501 (necesita el volumen de datos de docker-up)
docker-dashboard:
	docker compose build lakehouse
	docker compose up --detach --wait dashboard

# Comprueba las 3 pestañas dentro del contenedor del dashboard
docker-dashboard-check:
	docker compose exec dashboard python scripts/check_dashboard.py
