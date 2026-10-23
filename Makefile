SHELL := /bin/bash

.PHONY: install test test-spark test-report

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
