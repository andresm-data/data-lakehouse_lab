"""Consistencia entre Parquet, Delta e Iceberg y casos borde de las
escrituras con Spark.

El benchmark de la semana 3 compara los tres formatos, así que aquí se
comprueba que, con la misma entrada, los tres devuelven los mismos datos.
"""
from datetime import date

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from lakehouse.spark.read_write import (
    SAMPLE_ROWS, SAMPLE_SCHEMA, read_parquet, roundtrip_delta,
    roundtrip_iceberg, roundtrip_parquet, spark_name, verify_write,
    write_delta, write_iceberg, write_parquet,
)


NULL_ROWS = [
    (10, None, 5.0, date(2025, 1, 1)),
    (11, 'Bronx', None, None),
    (12, None, None, None),
]


# =============================================================================
def write_all(spark: SparkSession, df: DataFrame, root, name: str) -> dict:
    """Escribe el DataFrame en los tres formatos y los relee con Spark."""
    parquet = root / 'parquet' / name
    delta = root / 'delta' / name
    identifier = f'lakehouse.{name}'

    write_parquet(df, parquet)
    write_delta(df, delta)
    write_iceberg(spark, df, identifier)

    return {
        'parquet': spark.read.parquet(str(parquet)),
        'delta': spark.read.format('delta').load(str(delta)),
        'iceberg': spark.table(spark_name(identifier)),
    }


# =============================================================================
def rows(df: DataFrame) -> list[tuple]:
    """Filas como tuplas, en un orden estable aunque haya nulos."""
    return sorted((tuple(r) for r in df.collect()), key=repr)


# =============================================================================
def fields(df: DataFrame) -> list[tuple[str, str]]:
    return [(f.name, f.dataType.simpleString()) for f in df.schema.fields]


# =============================================================================
class TestConsistencia:

    def test_misma_muestra_en_los_tres_formatos(self, spark, lake):
        df = spark.createDataFrame(SAMPLE_ROWS + NULL_ROWS, SAMPLE_SCHEMA)
        read = write_all(spark, df, lake['root'], 'consistencia')

        expected = rows(df)

        for fmt, out in read.items():
            assert rows(out) == expected, fmt
            assert fields(out) == fields(df), fmt

    def test_origenes_sin_spark_coinciden(self, spark, parquet_source,
                                          delta_source, iceberg_source):
        parquet = spark.read.parquet(str(parquet_source))
        delta = spark.read.format('delta').load(str(delta_source))
        iceberg = spark.table(spark_name(iceberg_source))

        assert rows(parquet) == rows(delta) == rows(iceberg)
        assert fields(parquet) == fields(delta) == fields(iceberg)

    def test_misma_agregacion(self, spark, lake):
        df = spark.createDataFrame(SAMPLE_ROWS + NULL_ROWS, SAMPLE_SCHEMA)
        read = write_all(spark, df, lake['root'], 'agregacion')

        totals = {
            fmt: rows(out.groupBy('zona').sum('monto'))
            for fmt, out in read.items()
        }

        assert totals['parquet'] == totals['delta'] == totals['iceberg']

    def test_roundtrips_devuelven_lo_mismo(self, spark, catalog, lake):
        root = lake['root']
        results = [
            roundtrip_parquet(spark, root / 'parquet' / 'rt'),
            roundtrip_delta(spark, root / 'delta' / 'rt'),
            roundtrip_iceberg(spark, catalog, 'lakehouse.rt'),
        ]

        assert all(r == results[0] for r in results)
        assert results[0]['spark_ok'] and results[0]['otro_motor_ok']


# =============================================================================
class TestEntradaVacia:

    def test_tablas_vacias_conservan_el_esquema(self, spark, lake):
        df = spark.createDataFrame([], SAMPLE_SCHEMA)
        read = write_all(spark, df, lake['root'], 'vacia_fmt')

        for fmt, out in read.items():
            assert out.count() == 0, fmt
            assert fields(out) == fields(df), fmt

    def test_check_con_cero_filas(self, spark, lake):
        path = lake['root'] / 'parquet' / 'vacia_check'
        write_parquet(spark.createDataFrame([], SAMPLE_SCHEMA), path)

        r = read_parquet(spark, path)

        assert r['filas'] == 0 and r['filas_ok'] and r['esquema_ok']

    def test_verify_write_vacio(self, spark):
        r = verify_write(spark.createDataFrame([], SAMPLE_SCHEMA), [], [])

        assert r == {'filas': 0, 'spark_ok': True, 'otro_motor_ok': True}


# =============================================================================
class TestNulos:

    def test_nulos_se_conservan(self, spark, lake):
        df = spark.createDataFrame(NULL_ROWS, SAMPLE_SCHEMA)
        read = write_all(spark, df, lake['root'], 'nulos')

        for fmt, out in read.items():
            assert out.filter('zona IS NULL').count() == 2, fmt
            assert out.filter('monto IS NULL').count() == 2, fmt
            assert out.filter('fecha IS NULL').count() == 2, fmt

    def test_verify_write_detecta_nulo_perdido(self, spark):
        df = spark.createDataFrame(NULL_ROWS, SAMPLE_SCHEMA)
        changed = [(10, '', 5.0, date(2025, 1, 1)), *NULL_ROWS[1:]]

        r = verify_write(df, NULL_ROWS, changed)

        assert r['spark_ok'] and not r['otro_motor_ok']


# =============================================================================
WRONG_SCHEMA = StructType([
    StructField('id', StringType(), False),
    StructField('otra', LongType(), True),
])


class TestEsquemaIncorrecto:

    def test_delta_rechaza_append_con_otro_esquema(self, spark, lake):
        path = lake['root'] / 'delta' / 'esquema'
        write_delta(spark.createDataFrame(SAMPLE_ROWS, SAMPLE_SCHEMA), path)
        wrong = spark.createDataFrame([('x', 1)], WRONG_SCHEMA)

        with pytest.raises(AnalysisException):
            wrong.write.format('delta').mode('append').save(str(path))

        assert spark.read.format('delta').load(str(path)).count() == 3

    def test_iceberg_rechaza_append_con_otro_esquema(self, spark):
        identifier = 'lakehouse.esquema'
        write_iceberg(spark, spark.createDataFrame(SAMPLE_ROWS, SAMPLE_SCHEMA),
                      identifier)
        wrong = spark.createDataFrame([('x', 1)], WRONG_SCHEMA)

        with pytest.raises(AnalysisException):
            wrong.writeTo(spark_name(identifier)).append()

        assert spark.table(spark_name(identifier)).count() == 3

    def test_parquet_acepta_otro_esquema_y_falla_al_leer(self, spark, lake):
        path = lake['root'] / 'parquet' / 'esquema'
        write_parquet(spark.createDataFrame(SAMPLE_ROWS, SAMPLE_SCHEMA), path)
        wrong = spark.createDataFrame([('x', 1)], WRONG_SCHEMA)

        # Parquet no tiene log ni catálogo que valide: el append se acepta y
        # el directorio queda con archivos incompatibles (`id` long y string).
        wrong.write.mode('append').parquet(str(path))

        with pytest.raises(Exception, match='(?i)merge|incompatible'):
            spark.read.option('mergeSchema', 'true').parquet(str(path)).count()

    def test_parquet_ruta_inexistente(self, spark, lake):
        with pytest.raises(AnalysisException):
            read_parquet(spark, lake['root'] / 'parquet' / 'no_existe')

