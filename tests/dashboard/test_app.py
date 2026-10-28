"""Pruebas de la app de Streamlit (lakehouse.dashboard.app) con AppTest."""
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import lakehouse.dashboard
from lakehouse.benchmarks import results_db


# =============================================================================
APP = Path(lakehouse.dashboard.__file__).with_name('app.py')


def _run(monkeypatch, db: Path) -> AppTest:
    monkeypatch.setattr(results_db, 'RESULTS_DB', db)

    return AppTest.from_file(str(APP), default_timeout=30).run()


@pytest.fixture
def app(monkeypatch, db) -> AppTest:
    return _run(monkeypatch, db)


# =============================================================================
def test_abre_sin_errores(app):
    assert not app.exception
    assert app.title[0].value == 'Lakehouse Lab'


def test_tiene_la_pestana_de_benchmarks(app):
    assert [t.label for t in app.tabs] == ['Benchmarks']


def test_sin_base_muestra_como_generarla(monkeypatch, tmp_path):
    app = _run(monkeypatch, tmp_path / 'no_existe.duckdb')

    assert not app.exception
    assert 'make bench-formats' in app.info[0].value
    assert not app.dataframe


# =============================================================================
def test_muestra_la_ejecucion_mas_reciente(app, saved_runs):
    sizes, times = app.dataframe

    assert app.selectbox[0].value == saved_runs[0]
    assert list(sizes.value['formato']) == ['parquet', 'delta', 'iceberg']
    assert len(times.value) == 6


def test_cambia_de_ejecucion(app, saved_runs):
    app.selectbox[0].set_value(saved_runs[1]).run()

    assert not app.exception
    assert len(app.dataframe[1].value) == 3  # la ejecución vieja solo tiene q1
