"""Pruebas unitarias de lakehouse.benchmarks.compression."""
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakehouse.benchmarks.compression import (
    DEFAULT_CODECS,
    Result,
    benchmark,
    main,
    report,
)


# =============================================================================
@pytest.fixture
def origin(tmp_path: Path) -> Path:
    """Archivo Parquet de origen con un timestamp, como los datos de taxis."""
    path = tmp_path / 'origen.parquet'
    pq.write_table(pa.table({
        'id': pa.array(range(100), pa.int64()),
        'fecha': pa.array(
            [datetime(2025, 1, 1, 0, 0, i % 60) for i in range(100)],
            pa.timestamp('us')
        ),
        'monto': [i * 1.5 for i in range(100)],
    }), path)
    return path


# =============================================================================
# benchmark
# =============================================================================
class TestBenchmark:

    @pytest.mark.parametrize('format_', ['parquet', 'csv'])
    def test_mide_cada_codec_por_defecto(self, origin, tmp_path, format_):
        workdir = tmp_path / 'trabajo'

        results = benchmark(origin, workdir, format_)

        assert [r.compression for r in results] == list(DEFAULT_CODECS[format_])
        assert all(r.size_bytes > 0 for r in results)
        assert all(r.write_s > 0 and r.read_s > 0 for r in results)
        assert list(workdir.iterdir()) == []

    def test_codecs_seleccionados_y_conservar(self, origin, tmp_path):
        workdir = tmp_path / 'trabajo'

        results = benchmark(
            origin, workdir, 'csv', ['none', 'gzip'], repetitions=2, keep=True
        )

        assert [r.compression for r in results] == ['none', 'gzip']
        assert sorted(p.name for p in workdir.iterdir()) == [
            'origen_gzip.csv.gz', 'origen_none.csv'
        ]
        assert results[0].size_bytes == (workdir / 'origen_none.csv').stat().st_size

    def test_codec_invalido_falla_antes_de_escribir(self, origin, tmp_path):
        workdir = tmp_path / 'trabajo'

        with pytest.raises(ValueError, match='no válida'):
            benchmark(origin, workdir, 'parquet', ['zstd', 'bz2'])

        assert not workdir.exists()

    def test_repeticiones_invalidas(self, origin, tmp_path):
        with pytest.raises(ValueError, match='repeticiones'):
            benchmark(origin, tmp_path / 'trabajo', repetitions=0)


# =============================================================================
# report
# =============================================================================
def test_report_incluye_tabla(origin):
    results = [Result('zstd', 1.234, 3 * 1024 ** 2, 0.5)]

    text = report(results, origin, 'parquet', 3, datetime(2026, 10, 6, 19, 40, 5))

    assert '- Fecha: 2026-10-06 19:40:05' in text
    assert '- Repeticiones: 3' in text
    assert '| zstd | 1.23 | 3.0 | 0.50 |' in text


# =============================================================================
# main
# =============================================================================
class TestMain:

    def _run(self, monkeypatch, *args: str) -> int:
        monkeypatch.setattr(sys, 'argv', ['lh-bench-compression', *args])
        return main()

    def test_guarda_reporte_con_fecha(self, monkeypatch, capsys, origin, tmp_path):
        out_dir = tmp_path / 'docs'

        code = self._run(
            monkeypatch, str(origin), '-c', 'zstd', 'snappy',
            '--dir-trabajo', str(tmp_path / 'trabajo'),
            '--dir-salida', str(out_dir)
        )

        assert code == 0
        [target] = out_dir.iterdir()
        assert target.name.startswith('compresion_parquet_')
        datetime.strptime(target.stem.removeprefix('compresion_parquet_'), '%Y%m%d_%H%M%S')
        text = target.read_text(encoding='utf-8')
        assert '| zstd |' in text and '| snappy |' in text
        assert f'Listo -> {target}' in capsys.readouterr().out

    def test_error_devuelve_1(self, monkeypatch, capsys, tmp_path):
        code = self._run(
            monkeypatch, str(tmp_path / 'no_existe.parquet'),
            '--dir-salida', str(tmp_path / 'docs')
        )

        assert code == 1
        assert 'Error:' in capsys.readouterr().err
        assert not (tmp_path / 'docs').exists()
