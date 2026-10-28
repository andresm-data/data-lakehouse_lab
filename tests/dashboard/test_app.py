"""Pruebas de la app de Streamlit (lakehouse.dashboard.app) con AppTest."""
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import lakehouse.dashboard


# =============================================================================
APP = Path(lakehouse.dashboard.__file__).with_name('app.py')


@pytest.fixture
def app() -> AppTest:
    return AppTest.from_file(str(APP), default_timeout=30).run()


# =============================================================================
def test_abre_sin_errores(app):
    assert not app.exception
    assert app.title[0].value == 'Lakehouse Lab'


def test_tiene_la_pestana_de_benchmarks(app):
    assert [t.label for t in app.tabs] == ['Benchmarks']
