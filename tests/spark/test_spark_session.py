"""Pruebas unitarias de lakehouse.spark.spark_session."""
from pathlib import Path

from lakehouse.spark.spark_session import (
    ICEBERG_PACKAGE, ICEBERG_VERSION, iceberg_catalog_conf, versions,
)


# =============================================================================
class TestIcebergCatalogConf:

    def test_catalogo_jdbc_sobre_sqlite(self, tmp_path: Path):
        conf = iceberg_catalog_conf('cat', tmp_path / 'c.db', tmp_path / 'wh')

        assert conf['spark.sql.catalog.cat'] == (
            'org.apache.iceberg.spark.SparkCatalog'
        )
        assert conf['spark.sql.catalog.cat.type'] == 'jdbc'
        assert conf['spark.sql.catalog.cat.uri'] == (
            f'jdbc:sqlite:{(tmp_path / "c.db").resolve()}'
        )
        assert conf['spark.sql.catalog.cat.warehouse'].startswith('file://')
        assert conf['spark.sql.catalog.cat.jdbc.schema-version'] == 'V1'


# =============================================================================
class TestSession:

    def test_extensiones_y_catalogos(self, spark):
        conf = spark.conf

        assert 'DeltaSparkSessionExtension' in conf.get('spark.sql.extensions')
        assert 'IcebergSparkSessionExtensions' in conf.get('spark.sql.extensions')
        assert conf.get('spark.sql.catalog.spark_catalog').endswith('DeltaCatalog')
        assert conf.get('spark.sql.catalog.lakehouse.type') == 'jdbc'

    def test_crea_directorios(self, lake, spark):
        assert lake['warehouse'].is_dir()

    def test_versiones(self, spark):
        v = versions(spark)

        assert v['Spark'].startswith('4.1.')
        assert v['Scala'].startswith('2.13.')
        assert v['Iceberg (runtime)'] == ICEBERG_VERSION
        assert ICEBERG_VERSION in ICEBERG_PACKAGE
        assert v['Java'] and v['Delta (delta-spark)'] and v['Python']
