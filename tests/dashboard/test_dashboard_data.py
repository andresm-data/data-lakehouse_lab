"""Pruebas unitarias de lakehouse.dashboard.data."""
import pytest

from lakehouse.dashboard.data import load_sizes, load_times, runs


# =============================================================================
def test_runs_de_la_mas_reciente_a_la_mas_antigua(db, saved_runs):
    assert runs(db) == list(saved_runs)


def test_runs_sin_base(tmp_path):
    assert runs(tmp_path / 'no_existe.duckdb') == []


# =============================================================================
def test_load_times_en_ms_y_en_orden(db, saved_runs):
    df = load_times(db, saved_runs[0])

    assert list(zip(df['consulta'], df['formato'])) == [
        ('q1', 'parquet'), ('q1', 'delta'), ('q1', 'iceberg'),
        ('q2', 'parquet'), ('q2', 'delta'), ('q2', 'iceberg'),
    ]
    q2_parquet = df.iloc[3]
    assert q2_parquet['mediana_ms'] == pytest.approx(60)
    assert (q2_parquet['min_ms'], q2_parquet['max_ms']) == pytest.approx((50, 70))
    assert q2_parquet['filas'] == 3
    assert set(df['repeticiones']) == {3}


def test_load_times_solo_de_la_ejecucion_pedida(db, saved_runs):
    assert len(load_times(db, saved_runs[1])) == 3


# =============================================================================
def test_load_sizes_en_mb_y_en_orden(db, saved_runs):
    df = load_sizes(db, saved_runs[0])

    assert list(df['formato']) == ['parquet', 'delta', 'iceberg']
    assert list(df['archivos']) == [1, 6, 5]
    assert list(df['actual_mb']) == [2, 1, 3]
    assert list(df['disco_mb']) == [2, 5, 4]
