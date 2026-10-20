"""Compacta los archivos de la tabla Delta (OPTIMIZE) y mide el antes y después."""
import argparse
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from lakehouse.delta.merge_table import METRICS_DIR, TARGET
from lakehouse.delta.time_travel import _markdown_table


# =============================================================================
METRICS = [
    'numFilesAdded', 'numFilesRemoved', 'partitionsOptimized',
    'numBatches', 'totalConsideredFiles', 'totalFilesSkipped',
]
MB = 1024 * 1024


# =============================================================================
def describe(target: Path) -> dict:
    """Mide los archivos activos de la versión actual de la tabla.

    Es el equivalente en `deltalake` de `DESCRIBE DETAIL` (`numFiles`,
    `sizeInBytes`): solo cuenta los archivos que la versión actual usa, no
    los que siguen en disco esperando a VACUUM.

    Args:
        target (Path): Tabla Delta.

    Returns:
        dict: `version`, `archivos`, `bytes`, `filas`, `promedio` (bytes por
            archivo) y `particiones` (por cada partición, sus archivos y
            bytes).
    """
    table = DeltaTable(target)
    actions = pa.table(table.get_add_actions(flatten=True)).to_pydict()
    sizes = actions['size_bytes']

    partitions: dict[str, list[int]] = {}

    for path, size in zip(actions['path'], sizes):
        key = str(Path(path).parent)
        files, total = partitions.get(key, [0, 0])
        partitions[key] = [files + 1, total + size]

    return {
        'version': table.version(),
        'archivos': len(sizes),
        'bytes': sum(sizes),
        'filas': sum(actions['num_records']),
        'promedio': sum(sizes) / len(sizes) if sizes else 0,
        'particiones': dict(sorted(partitions.items())),
    }


# =============================================================================
def optimize(target: Path, target_size: int | None = None) -> dict:
    """Ejecuta OPTIMIZE (compactación) sobre la tabla.

    En cada partición une los archivos pequeños en archivos de hasta
    `target_size` bytes. Los archivos reemplazados quedan en disco hasta que
    VACUUM los borre.

    Args:
        target (Path): Tabla Delta.
        target_size (int | None): Tamaño objetivo por archivo en bytes; con
            `None` se usa el de la tabla (100 MB por defecto).

    Returns:
        dict: Métricas que devuelve `deltalake`, como `numFilesAdded` y
            `numFilesRemoved`.
    """
    return DeltaTable(target).optimize.compact(target_size=target_size)


# =============================================================================
def _mb(value: float) -> str:
    """Bytes como texto en MB con 2 decimales."""
    return f'{value / MB:,.2f} MB'


# =============================================================================
def render(target: Path, before: dict, after: dict, metrics: dict) -> str:
    """Arma el informe en Markdown con la comparación antes y después.

    Args:
        target (Path): Tabla Delta.
        before (dict): Salida de `describe` antes de OPTIMIZE.
        after (dict): Salida de `describe` después de OPTIMIZE.
        metrics (dict): Salida de `optimize`.

    Returns:
        str: Contenido del informe.
    """
    v0, v1 = before['version'], after['version']
    partitions = sorted(before['particiones'] | after['particiones'])

    def _files(stats: dict, key: str) -> str:
        files, size = stats['particiones'].get(key, [0, 0])
        return f'{files} ({_mb(size)})'

    lines = [
        '# OPTIMIZE en Delta: compactación de archivos',
        '',
        f'- Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'- Tabla: `{target}`',
        '- Comando: `DeltaTable(ruta).optimize.compact()`',
        '',
        '## Antes y después',
        '',
        *_markdown_table(
            ['Medida', f'Antes (v{v0})', f'Después (v{v1})'],
            [
                ['Nº de archivos', before['archivos'], after['archivos']],
                ['Tamaño promedio por archivo', _mb(before['promedio']),
                 _mb(after['promedio'])],
                ['Tamaño total', _mb(before['bytes']), _mb(after['bytes'])],
                ['Filas', f'{before["filas"]:,}', f'{after["filas"]:,}'],
            ],
        ),
        '',
        '## Archivos por partición',
        '',
        *_markdown_table(
            ['Partición', 'Antes', 'Después'],
            [[f'`{p}`', _files(before, p), _files(after, p)]
             for p in partitions],
        ),
        '',
        '## Métricas de OPTIMIZE',
        '',
        *_markdown_table(
            ['Métrica', 'Valor'],
            [[f'`{k}`', metrics[k]] for k in METRICS if k in metrics],
        ),
        '',
    ]

    return '\n'.join(lines)


# =============================================================================
def save_report(text: str, out_dir: Path) -> Path:
    """Guarda el informe como `optimize_<AAAAMMDD_HHMMSS>.md`.

    Args:
        text (str): Contenido del informe.
        out_dir (Path): Directorio de salida; se crea si no existe.

    Returns:
        Path: Ruta del informe.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'optimize_{datetime.now():%Y%m%d_%H%M%S}.md'
    path.write_text(text)

    return path


# =============================================================================
def main() -> int:
    """Mide la tabla, ejecuta OPTIMIZE, vuelve a medir y guarda el informe.

    Returns:
        int: `0` si las filas no cambian, `1` si cambian.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tabla', type=Path, default=TARGET)
    parser.add_argument('--salida', type=Path, default=METRICS_DIR)
    parser.add_argument('--tamano-mb', type=int, default=None,
                        help='tamaño objetivo por archivo en MB '
                             '(por defecto, el de la tabla: 100 MB)')
    args = parser.parse_args()

    target_size = args.tamano_mb * MB if args.tamano_mb else None

    before = describe(args.tabla)
    metrics = optimize(args.tabla, target_size)
    after = describe(args.tabla)
    path = save_report(render(args.tabla, before, after, metrics), args.salida)

    print(f'Archivos: {before["archivos"]} -> {after["archivos"]}')
    print(f'Tamaño promedio: {_mb(before["promedio"])} -> '
          f'{_mb(after["promedio"])}')
    print(f'Tamaño total: {_mb(before["bytes"])} -> {_mb(after["bytes"])}')
    print(f'Informe -> {path}')

    return 0 if before['filas'] == after['filas'] else 1


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
