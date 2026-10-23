"""Pruebas unitarias de lakehouse.versions."""
import tomllib
from importlib.metadata import PackageNotFoundError
from pathlib import Path

import pytest

from lakehouse import versions as v


PYPROJECT = Path(__file__).resolve().parents[1] / 'pyproject.toml'


# =============================================================================
class TestConstantes:

    def test_paquete_de_iceberg_coincide_con_spark_y_scala(self):
        assert v.SPARK_MINOR == '4.1'
        assert v.ICEBERG_PACKAGE == (
            'org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:'
            f'{v.ICEBERG_VERSION}'
        )
        assert v.SQLITE_JDBC_PACKAGE.endswith(f':{v.SQLITE_JDBC_VERSION}')

    def test_pyproject_fija_las_mismas_versiones(self):
        deps = tomllib.loads(PYPROJECT.read_text())['project']['dependencies']

        for package, expected in v.PYTHON_PACKAGES.items():
            assert f'{package} (=={expected})' in deps

    def test_pyproject_sin_rangos(self):
        project = tomllib.loads(PYPROJECT.read_text())
        deps = (project['project']['dependencies']
                + project['dependency-groups']['dev'])

        assert all('(==' in d for d in deps), deps


# =============================================================================
class TestJavaMajor:

    @pytest.mark.parametrize('text, major', [
        ('21.0.12.1', '21'), ('17.0.2', '17'), ('1.8.0_402', '8'), ('25', '25'),
    ])
    def test_java_major(self, text, major):
        assert v.java_major(text) == major


# =============================================================================
class TestCheckInstalled:

    def test_entorno_actual(self):
        assert v.check_installed() == []

    def test_version_distinta(self, monkeypatch):
        monkeypatch.setattr(v, 'version', lambda p: '0.0.1')

        problems = v.check_installed()

        assert len(problems) == 2
        assert 'pyspark 0.0.1 instalado, se espera 4.1.3' in problems

    def test_paquete_ausente(self, monkeypatch):
        def missing(package):
            raise PackageNotFoundError(package)

        monkeypatch.setattr(v, 'version', missing)

        assert v.check_installed()[0].startswith('pyspark no está instalado')
