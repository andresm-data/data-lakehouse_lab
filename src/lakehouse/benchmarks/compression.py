"""Compara códecs de compresión al convertir un archivo a Parquet o CSV."""
import argparse
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pyarrow as pa

from lakehouse.ingest._io import AVAILABLE_COMPRESSIONS, common_schema, scan
from lakehouse.ingest._validation import validate
from lakehouse.ingest.convert import convert


# =============================================================================
DEFAULT_CODECS: dict[str, tuple[str, ...]] = {
    'parquet': ('zstd', 'gzip', 'lz4', 'snappy'),
    'csv': ('zstd', 'gzip', 'lz4', 'bz2')
}
CSV_EXTENSIONS = {'none': '', 'gzip': '.gz', 'bz2': '.bz2', 'lz4': '.lz4', 'zstd': '.zst'}
MB = 1024 ** 2


# =============================================================================
@dataclass(frozen=True)
class Result:
    """Métricas de un códec; los tiempos son la mediana de las repeticiones."""
    compression: str
    write_s: float
    size_bytes: int
    read_s: float


# =============================================================================
def _target_name(origin: Path, format_: str, compression: str) -> str:
    """Nombre del archivo convertido, con la extensión que espera la lectura."""
    stem = origin.name.split('.')[0]

    if format_ == 'parquet':
        return f'{stem}_{compression}.parquet'

    return f'{stem}_{compression}.csv{CSV_EXTENSIONS[compression]}'


# =============================================================================
def _read(path: Path, schema: pa.Schema) -> float:
    """Lee el archivo completo por lotes y devuelve los segundos empleados."""
    start = time.perf_counter()

    for _ in scan([path], schema):
        pass

    return time.perf_counter() - start


# =============================================================================
def benchmark(
    origin: Path,
    workdir: Path,
    format_: str = 'parquet',
    compressions: list[str] | None = None,
    repetitions: int = 1,
    keep: bool = False
) -> list[Result]:
    """Convierte el origen con cada códec y mide escritura, tamaño y lectura.

    La escritura mide la llamada completa a `convert`, por lo que incluye
    la lectura del origen; con un origen Parquet ese costo es bajo y
    similar entre códecs. La lectura recorre por lotes el archivo recién
    escrito, así que suele servirse desde la caché de página del sistema
    operativo y mide sobre todo la descompresión y el decodificado.

    Args:
        origin: Archivo Parquet o CSV de origen.
        workdir: Directorio donde se escriben los archivos convertidos.
        format_ (opcional): Formato de destino ('parquet' o 'csv').
            Por defecto 'parquet'.
        compressions (opcional): Códecs a comparar. Si es `None`, se usan
            los de `DEFAULT_CODECS` para el formato. Por defecto `None`.
        repetitions (opcional): Veces que se mide cada códec; se reporta
            la mediana. Por defecto 1.
        keep (opcional): Si es `True`, conserva los archivos convertidos.
            Por defecto `False`.

    Returns:
        list[Result]: Métricas de cada códec, en el orden recibido.

    Raises:
        ValueError: Si `repetitions` es menor que 1.
        ValueError: Si algún códec no es válido para el formato; se valida
            antes de escribir nada.
    """
    if repetitions < 1:
        raise ValueError('El número de repeticiones debe ser al menos 1')

    compressions = list(compressions or DEFAULT_CODECS[format_])
    targets = {c: workdir / _target_name(origin, format_, c) for c in compressions}

    # Valida todos los códecs antes de empezar mediciones que pueden tardar
    for c, target in targets.items():
        validate(origin, target, format_, c, None, True)

    schema = common_schema([origin])
    workdir.mkdir(parents=True, exist_ok=True)
    results = []

    for c, target in targets.items():
        writes, reads = [], []

        try:
            for _ in range(repetitions):
                start = time.perf_counter()
                convert(origin, target, format_, c, overwrite=True)
                writes.append(time.perf_counter() - start)
                reads.append(_read(target, schema))

            size = target.stat().st_size

        finally:
            if not keep:
                target.unlink(missing_ok=True)

        results.append(Result(
            c, statistics.median(writes), size, statistics.median(reads)
        ))

    return results


# =============================================================================
def report(
    results: list[Result],
    origin: Path,
    format_: str,
    repetitions: int,
    started: datetime
) -> str:
    """Genera el reporte en Markdown con la tabla de resultados.

    Args:
        results: Métricas obtenidas con `benchmark`.
        origin: Archivo de origen.
        format_: Formato de destino.
        repetitions: Repeticiones por códec.
        started: Fecha y hora de inicio de la ejecución.

    Returns:
        str: Reporte en Markdown.
    """
    lines = [
        f'# Benchmark de compresión: {format_}',
        '',
        f'- Fecha: {started:%Y-%m-%d %H:%M:%S}',
        f'- Origen: `{origin}` ({origin.stat().st_size / MB:,.1f} MB)',
        f'- Repeticiones: {repetitions} (tiempos: mediana)',
        '',
        '| Compresión | Escritura (s) | Tamaño (MB) | Lectura (s) |',
        '|---|---:|---:|---:|',
    ]
    lines += [
        f'| {r.compression} | {r.write_s:.2f} | '
        f'{r.size_bytes / MB:,.1f} | {r.read_s:.2f} |'
        for r in results
    ]

    return '\n'.join(lines) + '\n'


# =============================================================================
def main() -> int:
    """Punto de entrada del comando `lh-bench-compression`.

    Returns:
        int: Código de salida del proceso: `0` si el benchmark fue exitoso,
            `1` si ocurrió un error.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        'origen', type=Path, help='Archivo Parquet o CSV de entrada'
    )
    parser.add_argument(
        '-f',
        '--formato',
        choices=AVAILABLE_COMPRESSIONS,
        default='parquet',
        help='Formato de salida (por defecto: parquet)'
    )
    parser.add_argument(
        '-c',
        '--compresiones',
        nargs='+',
        metavar='CODEC',
        help='Códecs a comparar. Por defecto, Parquet: ' +
        ' '.join(DEFAULT_CODECS['parquet']) + ' | CSV: ' +
        ' '.join(DEFAULT_CODECS['csv'])
    )
    parser.add_argument(
        '-r',
        '--repeticiones',
        type=int,
        default=1,
        help='Mediciones por códec; se reporta la mediana (por defecto: 1)'
    )
    parser.add_argument(
        '--dir-trabajo',
        type=Path,
        default=Path('data/benchmarks'),
        help='Directorio de los archivos convertidos '
        '(por defecto: data/benchmarks)'
    )
    parser.add_argument(
        '--dir-salida',
        type=Path,
        default=Path('docs/benchmarks'),
        help='Directorio del reporte (por defecto: docs/benchmarks)'
    )
    parser.add_argument(
        '--conservar',
        action='store_true',
        help='Conserva los archivos convertidos'
    )
    args = parser.parse_args()
    started = datetime.now()

    try:
        results = benchmark(
            args.origen, args.dir_trabajo, args.formato,
            args.compresiones, args.repeticiones, args.conservar
        )

    except (ValueError, OSError, pa.ArrowException) as e:
        print(f'Error: {e}', file=sys.stderr)

        return 1

    text = report(results, args.origen, args.formato, args.repeticiones, started)
    target = args.dir_salida / f'compresion_{args.formato}_{started:%Y%m%d_%H%M%S}.md'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding='utf-8')

    print(text)
    print(f'Listo -> {target}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
