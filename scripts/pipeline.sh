#!/usr/bin/env bash
# Ejecuta el proyecto completo, desde la descarga de los datos hasta los benchmarks.
# Los informes quedan en $LAKEHOUSE_DOCS_DIR (docs/) y los datos en $LAKEHOUSE_DATA_DIR (data/).
set -euo pipefail

DATA="${LAKEHOUSE_DATA_DIR:-data}"
TLC_URL="${TLC_URL:-https://d37ci6vzurychx.cloudfront.net}"
MONTHS=(2025-01 2025-02 2025-03)

step() { printf '\n=== %s ===\n' "$*"; }

# -----------------------------------------------------------------------------
step "1. Descarga de los datos de NYC TLC (solo los que faltan)"
mkdir -p "$DATA/landing"
for month in "${MONTHS[@]}"; do
    file="$DATA/landing/yellow_tripdata_${month}.parquet"
    [[ -s "$file" ]] || curl -fSL --retry 3 -o "$file" "$TLC_URL/trip-data/yellow_tripdata_${month}.parquet"
done
zones="$DATA/landing/taxi_zone_lookup.csv"
[[ -s "$zones" ]] || curl -fSL --retry 3 -o "$zones" "$TLC_URL/misc/taxi_zone_lookup.csv"
ls -lh "$DATA/landing"

# -----------------------------------------------------------------------------
step "2. Ingesta: partición por fecha y Parquet consolidado"
lh-partition-date "$DATA"/landing/yellow_tripdata_2025-0*.parquet "$DATA/raw/yellow_tripdata.parquet" \
    --columna tpep_pickup_datetime --granularidad month \
    --desde 2025-01-01 --hasta 2025-04-01 --sobrescribir
lh-merge "$DATA/raw/yellow_tripdata.parquet" "$DATA/interim/yellow_tripdata.parquet" --sobrescribir

step "3. Benchmark de compresión (Parquet)"
lh-bench-compression "$DATA/interim/yellow_tripdata.parquet" -r 3

# -----------------------------------------------------------------------------
step "4. Delta Lake: escritura, MERGE, time travel, esquema y mantenimiento"
lh-delta-pyarrow
lh-delta-merge --reset
lh-delta-time-travel
lh-delta-schema-evolution
lh-delta-optimize
lh-delta-vacuum --retencion-horas 0    # solo en un entorno de práctica

# -----------------------------------------------------------------------------
step "5. Iceberg: catálogo, tabla, snapshots y evolución de partición"
lh-iceberg-catalog
lh-iceberg-create
lh-iceberg-snapshots
lh-iceberg-ops

# -----------------------------------------------------------------------------
step "6. Spark con Delta e Iceberg"
lh-spark-read-write
lh-spark-update-month --mes 2025-02

# -----------------------------------------------------------------------------
step "7. Benchmark de consultas con DuckDB"
lh-bench-formats -r 10 --calentamiento 2

step "Pipeline completo"
