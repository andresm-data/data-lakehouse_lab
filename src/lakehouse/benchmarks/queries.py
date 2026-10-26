"""Consultas del benchmark y cómo DuckDB lee cada formato.

Cada consulta se escribe una sola vez contra la vista `trips`. Lo único que
cambia entre formatos es la función con la que se crea esa vista
(`read_parquet`, `delta_scan` o `iceberg_scan`), así que el texto SQL que
se mide es idéntico en los tres.

Las consultas no usan `congestion_surcharge` ni `Airport_fee`: en Delta e
Iceberg se reemplazaron sus nulos de febrero por 0.0 con Spark, pero en el
Parquet siguen nulos, y los resultados no coincidirían.
"""
import duckdb


# =============================================================================
VIEW = 'trips'
FORMATS = ('parquet', 'delta', 'iceberg')
EXTENSIONS = {'delta': 'delta', 'iceberg': 'iceberg'}
SCAN_FUNCTIONS = {
    'parquet': 'read_parquet',
    'delta': 'delta_scan',
    'iceberg': 'iceberg_scan',
}

QUERIES: dict[str, str] = {
    # Solo lee metadatos o una columna: mide el costo fijo de abrir la tabla.
    'q1_conteo': f"""
SELECT count(*) AS viajes
FROM {VIEW}
""",
    # Recorre toda la tabla y agrupa por las columnas de partición de Delta.
    'q2_por_mes': f"""
SELECT year, month,
       count(*) AS viajes,
       sum(total_amount) AS total,
       avg(trip_distance) AS distancia_media
FROM {VIEW}
GROUP BY year, month
ORDER BY year, month
""",
    # Filtra un mes por la fecha original: prueba el partition pruning.
    'q3_filtro_febrero': f"""
SELECT count(*) AS viajes,
       sum(fare_amount) AS tarifa,
       avg(tip_amount) AS propina_media
FROM {VIEW}
WHERE tpep_pickup_datetime >= TIMESTAMP '2025-02-01'
  AND tpep_pickup_datetime <  TIMESTAMP '2025-03-01'
""",
    # Agregación con muchos grupos, ordenada y con límite.
    'q4_top_zonas': f"""
SELECT PULocationID,
       count(*) AS viajes,
       avg(total_amount) AS total_medio
FROM {VIEW}
GROUP BY PULocationID
ORDER BY viajes DESC, PULocationID
LIMIT 10
""",
    # Expresión calculada sobre un timestamp en todas las filas.
    'q5_por_hora': f"""
SELECT hour(tpep_pickup_datetime) AS hora,
       count(*) AS viajes,
       avg(trip_distance) AS distancia_media
FROM {VIEW}
GROUP BY hora
ORDER BY hora
""",
    # Filtro sobre un valor y agregación con una división por fila.
    'q6_propina_por_pago': f"""
SELECT payment_type,
       count(*) AS viajes,
       avg(tip_amount / fare_amount) AS propina_relativa
FROM {VIEW}
WHERE fare_amount > 0
GROUP BY payment_type
ORDER BY payment_type
""",
}


# =============================================================================
def _quote(value: str) -> str:
    """Literal de texto SQL, con las comillas simples escapadas."""
    return "'" + value.replace("'", "''") + "'"


# =============================================================================
def source_sql(format_: str, location: str) -> str:
    """Expresión `FROM` con la que DuckDB lee la tabla en ese formato.

    Args:
        format_: 'parquet', 'delta' o 'iceberg'.
        location: Archivo Parquet, directorio de la tabla Delta o archivo
            `metadata.json` actual de la tabla Iceberg.

    Returns:
        str: Por ejemplo, `delta_scan('data/delta/tabla')`.

    Raises:
        ValueError: Si el formato no es uno de `FORMATS`.
    """
    if format_ not in SCAN_FUNCTIONS:
        raise ValueError(
            f'Formato no válido: {format_!r} (opciones: {", ".join(FORMATS)})'
        )

    return f'{SCAN_FUNCTIONS[format_]}({_quote(location)})'


# =============================================================================
def connect(format_: str, location: str) -> duckdb.DuckDBPyConnection:
    """Abre una conexión en memoria con la vista `trips` sobre la tabla.

    Carga la extensión de DuckDB que necesita el formato; la primera vez
    la descarga, así que requiere internet.

    Args:
        format_: 'parquet', 'delta' o 'iceberg'.
        location: Ubicación de la tabla (ver `source_sql`).

    Returns:
        duckdb.DuckDBPyConnection: Conexión lista para ejecutar `QUERIES`.
    """
    source = source_sql(format_, location)
    con = duckdb.connect()

    if format_ in EXTENSIONS:
        con.install_extension(EXTENSIONS[format_])
        con.load_extension(EXTENSIONS[format_])

    con.execute(f'CREATE VIEW {VIEW} AS SELECT * FROM {source}')

    return con
