"""Pruebas unitarias de lakehouse.benchmarks.queries."""
import math

import pytest

from lakehouse.benchmarks.queries import (
    FORMATS,
    QUERIES,
    VIEW,
    connect,
    source_sql,
)


# =============================================================================
# source_sql
# =============================================================================
class TestSourceSql:

    @pytest.mark.parametrize('format_, expected', [
        ('parquet', "read_parquet('a/b.parquet')"),
        ('delta', "delta_scan('a/b.parquet')"),
        ('iceberg', "iceberg_scan('a/b.parquet')"),
    ])
    def test_funcion_por_formato(self, format_, expected):
        assert source_sql(format_, 'a/b.parquet') == expected

    def test_escapa_comillas(self):
        assert source_sql('parquet', "o'neil.parquet") == (
            "read_parquet('o''neil.parquet')"
        )

    def test_formato_invalido(self):
        with pytest.raises(ValueError, match='no válido'):
            source_sql('csv', 'x.csv')


# =============================================================================
# QUERIES
# =============================================================================
def test_consultas_solo_usan_la_vista():
    for sql in QUERIES.values():
        assert f'FROM {VIEW}' in sql
        assert not any(f in sql for f in ('read_parquet', 'delta_scan', 'iceberg_scan'))


def test_consultas_no_usan_columnas_que_difieren():
    for sql in QUERIES.values():
        assert 'congestion_surcharge' not in sql
        assert 'Airport_fee' not in sql


# =============================================================================
# connect
# =============================================================================
@pytest.mark.parametrize('format_', FORMATS)
def test_connect_crea_la_vista(locations, trips, format_):
    con = connect(format_, locations[format_])

    assert con.sql(f'SELECT count(*) FROM {VIEW}').fetchall()[0][0] == trips.num_rows


def test_mismos_resultados_en_los_tres_formatos(locations):
    results = {
        f: {q: connect(f, locations[f]).sql(sql).fetchall() for q, sql in QUERIES.items()}
        for f in FORMATS
    }

    for q in QUERIES:
        base = results['parquet'][q]
        assert base, q

        for f in ('delta', 'iceberg'):
            other = results[f][q]
            assert len(other) == len(base), (f, q)

            for row_a, row_b in zip(base, other):
                for a, b in zip(row_a, row_b):
                    if isinstance(a, float):
                        assert math.isclose(a, b, rel_tol=1e-9), (f, q)
                    else:
                        assert a == b, (f, q)


def test_filtro_febrero_cuenta_solo_febrero(locations, trips):
    con = connect('iceberg', locations['iceberg'])
    expected = sum(1 for m in trips['month'].to_pylist() if m == 2)

    assert con.sql(QUERIES['q3_filtro_febrero']).fetchall()[0][0] == expected
