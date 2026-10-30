#!/usr/bin/env bash
# Demo completa dentro de Docker: en el host solo hace falta Docker con Compose.
# Construye la imagen, ejecuta el pipeline, las pruebas y el notebook, y deja
# el dashboard en http://localhost:8501 después de comprobar sus 3 pestañas.
#
# Para probarla desde cero: make demo-clean && make demo
# BUILD_FLAGS=--no-cache reconstruye la imagen sin usar la caché de Docker.
set -euo pipefail

BUILD_FLAGS="${BUILD_FLAGS:-}"
compose() { docker compose --profile dashboard --profile tests "$@"; }
step() { printf '\n##### %s #####\n' "$*"; }

start=$SECONDS

step "1/5 Imagen lakehouse-lab (Python, Poetry y Java 21)"
# shellcheck disable=SC2086  # BUILD_FLAGS puede traer varias opciones
compose build $BUILD_FLAGS lakehouse

step "2/5 Pipeline: descarga, ingesta, Delta, Iceberg, Spark y benchmarks"
compose run --rm lakehouse

step "3/5 Pruebas"
compose run --rm tests

step "4/5 Notebook final con los datos del pipeline"
compose run --rm lakehouse python scripts/run_notebook.py

step "5/5 Dashboard"
compose up --detach --wait dashboard
compose exec dashboard python scripts/check_dashboard.py

minutes=$(( (SECONDS - start) / 60 ))
printf '\nDemo completa en %d min.\n' "$minutes"
printf 'Dashboard: http://localhost:8501  (se detiene con: docker compose stop dashboard)\n'
printf 'Informes en docs/ y notebook en notebooks/comparacion_formatos.ipynb\n'
