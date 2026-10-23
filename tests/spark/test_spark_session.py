"""Pruebas unitarias de lakehouse.spark.spark_session."""
from pathlib import Path

import pytest

from lakehouse import versions as pinned
from lakehouse.spark import spark_session
from lakehouse.spark.spark_session import (
    build_spark_session, iceberg_catalog_conf, versions,
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

        assert v['Spark'] == pinned.SPARK_VERSION
        assert v['Scala'].startswith(f'{pinned.SCALA_BINARY}.')
        assert v['Delta (delta-spark)'] == pinned.DELTA_VERSION
        assert v['Iceberg (runtime)'] == pinned.ICEBERG_VERSION
        assert pinned.java_major(v['Java']) in pinned.SUPPORTED_JAVA
        assert v['Python']

    def test_falla_con_versiones_incorrectas(self, monkeypatch, tmp_path):
        monkeypatch.setattr(spark_session, 'check_installed',
                            lambda: ['pyspark 4.2.0 instalado, se espera 4.1.3'])

        with pytest.raises(RuntimeError, match='pyspark 4.2.0'):
            build_spark_session('x', tmp_path / 'c.db', tmp_path / 'wh')
