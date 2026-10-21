"""Pruebas unitarias de lakehouse.spark.read_write."""
import sys

import pyarrow as pa

from lakehouse.spark import read_write
from lakehouse.spark.read_write import (
    all_ok, check, main, read_delta, read_iceberg, render, roundtrip_delta,
    roundtrip_iceberg, sample, spark_name,
)


# =============================================================================
class TestSparkName:

    def test_antepone_el_catalogo(self):
        assert spark_name('lakehouse.tabla') == 'lakehouse.lakehouse.tabla'


# =============================================================================
class TestCheck:

    def test_coincide(self, spark):
        df = sample(spark)
        schema = pa.schema([('id', pa.int64()), ('zona', pa.string()),
                            ('monto', pa.float64()), ('fecha', pa.date32())])

        r = check(df, 3, schema)

        assert r['filas_ok'] and r['esquema_ok']
        assert r['diferencias'] == []

    def test_detecta_diferencias(self, spark):
        df = sample(spark)
        schema = pa.schema([('id', pa.int32()), ('zona', pa.string()),
                            ('monto', pa.float64()), ('fecha', pa.date32())])

        r = check(df, 4, schema)

        assert not r['filas_ok']
        assert not r['esquema_ok']
        assert ('id', 'bigint') in r['diferencias']
        assert ('id', 'int') in r['diferencias']


# =============================================================================
class TestRead:

    def test_lee_delta_de_deltalake(self, spark, delta_source):
        r = read_delta(spark, delta_source)

        assert r['filas'] == 4
        assert r['filas_ok'] and r['esquema_ok']
        assert ('tpep_pickup_datetime', 'timestamp_ntz') in r['columnas']

    def test_lee_iceberg_de_pyiceberg(self, spark, catalog, iceberg_source):
        r = read_iceberg(spark, catalog, iceberg_source)

        assert r['filas'] == 4
        assert r['filas_ok'] and r['esquema_ok']

    def test_iceberg_sin_snapshots(self, spark, catalog):
        identifier = 'lakehouse.vacia'
        catalog.create_table(identifier, schema=pa.schema([('id', pa.int64())]))

        r = read_iceberg(spark, catalog, identifier)

        assert r['filas'] == 0 and r['filas_ok']


# =============================================================================
class TestRoundtrip:

    def test_delta(self, spark, lake):
        r = roundtrip_delta(spark, lake['root'] / 'delta' / 'smoke')

        assert r == {'filas': 3, 'spark_ok': True, 'otro_motor_ok': True}

    def test_delta_sobrescribe(self, spark, lake):
        path = lake['root'] / 'delta' / 'smoke2'
        roundtrip_delta(spark, path)

        assert roundtrip_delta(spark, path)['filas'] == 3
        assert spark.read.format('delta').load(str(path)).count() == 3

    def test_iceberg(self, spark, catalog):
        r = roundtrip_iceberg(spark, catalog, 'nuevo_ns.smoke')

        assert r == {'filas': 3, 'spark_ok': True, 'otro_motor_ok': True}
        assert ('nuevo_ns',) in catalog.list_namespaces()

    def test_iceberg_reemplaza(self, spark, catalog):
        roundtrip_iceberg(spark, catalog, 'lakehouse.smoke')
        r = roundtrip_iceberg(spark, catalog, 'lakehouse.smoke')

        assert r['spark_ok']
        assert spark.table('lakehouse.lakehouse.smoke').count() == 3


# =============================================================================
def _results(ok: bool = True) -> dict:
    lectura = {'filas': 5, 'filas_esperadas': 5, 'columnas': [('id', 'bigint')],
               'filas_ok': True, 'esquema_ok': ok,
               'diferencias': [] if ok else [('id', 'int')]}
    escritura = {'filas': 3, 'spark_ok': True, 'otro_motor_ok': True}

    return {'delta_lectura': lectura, 'iceberg_lectura': lectura,
            'delta_escritura': escritura, 'iceberg_escritura': escritura}


NAMES = {'delta_lectura': 'd', 'iceberg_lectura': 'i',
         'delta_escritura': 'dw', 'iceberg_escritura': 'iw'}


class TestReport:

    def test_all_ok(self):
        assert all_ok(_results())
        assert not all_ok(_results(ok=False))

    def test_render(self):
        text = render({'Spark': '4.1.3'}, _results(), NAMES)

        assert '| Spark | 4.1.3 |' in text
        assert '- Resultado: OK' in text
        assert 'Diferencias' not in text

    def test_render_con_fallas(self):
        text = render({}, _results(ok=False), NAMES)

        assert '- Resultado: FALLA' in text
        assert "Diferencias: [('id', 'int')]" in text


# =============================================================================
class TestMain:

    def _run(self, monkeypatch, spark, lake, argv):
        monkeypatch.setattr(read_write, 'build_spark_session',
                            lambda *a, **k: spark)
        monkeypatch.setattr(spark, 'stop', lambda: None)
        monkeypatch.setattr(sys, 'argv', ['lh-spark-read-write', *argv])

        return main()

    def test_exito(self, monkeypatch, spark, lake, delta_source,
                   iceberg_source, capsys):
        out = lake['root'] / 'docs'
        argv = ['--catalogo', str(lake['db']),
                '--warehouse', str(lake['warehouse']),
                '--delta', str(delta_source), '--iceberg', iceberg_source,
                '--delta-destino', str(lake['root'] / 'delta' / 'main'),
                '--iceberg-destino', 'lakehouse.main', '--salida', str(out)]

        assert self._run(monkeypatch, spark, lake, argv) == 0

        report = next(out.glob('read_write_*.md')).read_text()
        assert '- Resultado: OK' in report
        assert 'Informe ->' in capsys.readouterr().out

    def test_falla(self, monkeypatch, spark, lake, delta_source,
                   iceberg_source, capsys):
        monkeypatch.setattr(read_write, 'read_delta',
                            lambda *a: _results(ok=False)['delta_lectura'])
        argv = ['--catalogo', str(lake['db']),
                '--warehouse', str(lake['warehouse']),
                '--delta', str(delta_source), '--iceberg', iceberg_source,
                '--delta-destino', str(lake['root'] / 'delta' / 'main2'),
                '--iceberg-destino', 'lakehouse.main2',
                '--salida', str(lake['root'] / 'docs2')]

        assert self._run(monkeypatch, spark, lake, argv) == 1
        assert 'alguna comprobación falló' in capsys.readouterr().err
