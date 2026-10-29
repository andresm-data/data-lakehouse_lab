"""Pruebas de la app de Streamlit (lakehouse.dashboard.app) con AppTest."""
from datetime import timedelta
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

import lakehouse.dashboard
from lakehouse.benchmarks import results_db
from lakehouse.dashboard import time_travel
from lakehouse.dashboard.time_travel import Source


# =============================================================================
APP = Path(lakehouse.dashboard.__file__).with_name('app.py')
TABS = ['Benchmarks', 'Snapshots y time travel']


def _run(monkeypatch, db: Path, tables: list[Source] | None = None) -> AppTest:
    """Abre la app con la base de resultados y las tablas indicadas.

    Sin tablas, la pestaña de time travel apunta a una tabla que no existe,
    para no leer las de `data/`.
    """
    monkeypatch.setattr(results_db, 'RESULTS_DB', db)
    monkeypatch.setattr(time_travel, 'TABLES', tables or [
        Source('Delta · no existe', 'delta', str(db.parent / 'no_existe')),
    ])

    return AppTest.from_file(str(APP), default_timeout=30).run()


@pytest.fixture
def app(monkeypatch, db) -> AppTest:
    return _run(monkeypatch, db)


# =============================================================================
def test_abre_sin_errores(app):
    assert not app.exception
    assert app.title[0].value == 'Lakehouse Lab'


def test_tiene_las_pestanas(app):
    assert [t.label for t in app.tabs] == TABS


def test_sin_base_muestra_como_generarla(monkeypatch, tmp_path):
    app = _run(monkeypatch, tmp_path / 'no_existe.duckdb')
    benchmarks = app.tabs[0]

    assert not app.exception
    assert 'make bench-formats' in benchmarks.info[0].value
    assert not benchmarks.dataframe
    # Sin base, las demás pestañas se siguen mostrando.
    assert app.tabs[1].header[0].value == 'Snapshots y time travel'


# =============================================================================
def test_muestra_la_ejecucion_mas_reciente(app, saved_runs):
    sizes, times = app.tabs[0].dataframe

    assert app.tabs[0].selectbox[0].value == saved_runs[0]
    assert list(sizes.value['formato']) == ['parquet', 'delta', 'iceberg']
    assert len(times.value) == 6


def test_cambia_de_ejecucion(app, saved_runs):
    app.tabs[0].selectbox[0].set_value(saved_runs[1]).run()

    assert not app.exception
    assert len(app.tabs[0].dataframe[1].value) == 3  # la ejecución vieja solo tiene q1


# =============================================================================
def _charts(app: AppTest, tab: int = 0) -> list[dict]:
    """Figuras de Plotly de una pestaña, como JSON."""
    import json

    return [json.loads(c.proto.spec) for c in app.tabs[tab].get('plotly_chart')]


def test_muestra_los_dos_graficos(app):
    size, time = _charts(app)

    assert [t['name'] for t in size['data']] == ['Versión actual', 'En disco (con versiones anteriores)']
    assert [t['name'] for t in time['data']] == ['parquet', 'delta', 'iceberg']


def test_escala_logaritmica(app):
    app.tabs[0].toggle[0].set_value(True).run()

    assert _charts(app)[1]['layout']['yaxis']['type'] == 'log'


# =============================================================================
class TestSnapshots:

    @pytest.fixture
    def app(self, monkeypatch, db, delta_source, iceberg_source) -> AppTest:
        return _run(monkeypatch, db, [delta_source, iceberg_source[0]])

    @staticmethod
    def _select(app: AppTest, label: str, option: str | None = None, index: int = 0):
        """Elige una opción de un selectbox de la pestaña por su etiqueta."""
        box = next(s for s in app.tabs[1].selectbox if s.label == label)

        if option is None:
            option = box.options[index]

        box.set_value(option).run()

    def test_tabla_inexistente(self, monkeypatch, db):
        app = _run(monkeypatch, db)
        tab = app.tabs[1]

        assert not app.exception
        assert 'no existe' in tab.info[0].value
        assert '`make pipeline`' in tab.info[0].value
        assert not tab.dataframe

    def test_historial_de_delta(self, app):
        history = app.tabs[1].dataframe[0].value

        assert not app.exception
        assert list(history['version']) == ['0', '1', '2']
        assert list(history['actual']) == [False, False, True]

    def test_compara_la_version_0_con_la_actual(self, app):
        tab = app.tabs[1]
        comparison = tab.dataframe[1].value

        assert [m.value for m in tab.metric] == ['5', '6', '3']
        assert list(comparison['mes']) == ['2025-01', '2025-02', '2025-03']
        assert list(comparison['nulos_congestion_version']) == [1, 2, 0]
        assert list(comparison['nulos_congestion_actual']) == [1, 0, 0]

        chart = _charts(app, 1)[0]
        assert [t['name'] for t in chart['data']] == ['v0', 'Actual']

    def test_otra_version(self, app):
        self._select(app, 'Versión', index=1)

        assert not app.exception
        assert [m.value for m in app.tabs[1].metric][:2] == ['6', '6']

    def test_iceberg_snapshot_deshecho(self, app, iceberg_source):
        _, ids = iceberg_source
        self._select(app, 'Tabla', 'Iceberg · prueba')
        self._select(app, 'Versión', index=2)
        tab = app.tabs[1]

        assert not app.exception
        assert list(tab.dataframe[0].value['version']) == [str(i) for i in ids]
        assert 'rollback' in tab.info[0].value
        assert [m.value for m in tab.metric][:2] == ['6', '5']

    def test_version_borrada_por_vacuum(self, monkeypatch, db, vacuumed_source):
        app = _run(monkeypatch, db, [vacuumed_source])
        tab = app.tabs[1]

        assert not app.exception
        assert 'VACUUM' in tab.warning[0].value
        assert not tab.metric

    def test_por_fecha(self, app):
        tab = app.tabs[1]
        tab.radio[0].set_value('Fecha y hora (UTC)').run()

        # Por defecto, el minuto siguiente a la primera versión: la versión
        # vigente es la última creada dentro de ese minuto.
        assert not app.exception
        assert app.tabs[1].metric

    def test_por_fecha_antes_de_la_tabla(self, app):
        app.tabs[1].radio[0].set_value('Fecha y hora (UTC)').run()
        day = app.tabs[1].date_input(key='dia')
        day.set_value(day.value - timedelta(days=1)).run()

        assert not app.exception
        assert 'no existía' in app.tabs[1].warning[0].value
