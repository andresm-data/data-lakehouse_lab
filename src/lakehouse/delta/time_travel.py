"""Compara la versión 0 de la tabla Delta con la actual (time travel)."""
import argparse
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from deltalake import DeltaTable

from lakehouse.delta.merge_table import KEY, METRICS_DIR, TARGET, _partition


# =============================================================================
METRICS = [
    'num_added_rows', 'num_added_files', 'num_target_rows_updated',
    'num_target_rows_inserted', 'num_target_files_added',
    'num_target_files_removed',
]
SAMPLE = 5


# =============================================================================
def history_rows(target: Path) -> list[dict]:
    """Resume el historial de la tabla, de la versión más antigua a la actual.

    Args:
        target (Path): Tabla Delta.

    Returns:
        list[dict]: Una fila por versión con `version`, `fecha`, `operacion`
            y `metricas`; de las métricas solo se conservan las de `METRICS`.
    """
    rows = []

    for entry in sorted(DeltaTable(target).history(), key=lambda h: h['version']):
        metrics = entry.get('operationMetrics', {})
        rows.append({
            'version': entry['version'],
            'fecha': datetime.fromtimestamp(entry['timestamp'] / 1000),
            'operacion': entry['operation'],
            'metricas': {k: metrics[k] for k in METRICS if k in metrics},
        })

    return rows


# =============================================================================
def compare(target: Path, year: int, month: int) -> dict:
    """Compara `tip_amount` entre la versión 0 y la actual en una partición.

    Solo lee la clave y `tip_amount` de la partición pedida. Las claves
    repetidas en la versión 0 se excluyen, porque no identifican una sola
    fila.

    Args:
        target (Path): Tabla Delta.
        year (int): Año de la partición.
        month (int): Mes de la partición.

    Returns:
        dict: `version` (la actual), `filas_v0` y `filas_actual` (total de
            filas de cada versión), `cambiadas` (tabla con la clave y
            `tip_amount` en ambas versiones) e `insertadas` (filas de la
            partición cuya clave no existe en la versión 0).
    """
    columns = [*KEY, 'tip_amount']
    old = DeltaTable(target, version=0).to_pyarrow_dataset()
    new_table = DeltaTable(target)
    new = new_table.to_pyarrow_dataset()

    before = old.to_table(columns=columns, filter=_partition(year, month))
    after = new.to_table(columns=columns, filter=_partition(year, month))

    counts = before.group_by(KEY).aggregate([([], 'count_all')])
    unique = counts.filter(pc.field('count_all') == 1).select(KEY)
    before_unique = before.join(unique, KEY, join_type='inner')

    joined = before_unique.rename_columns([*KEY, 'tip_v0']).join(
        after.rename_columns([*KEY, 'tip_actual']), KEY, join_type='inner'
    )
    changed = joined.filter(pc.field('tip_v0') != pc.field('tip_actual'))
    inserted = after.join(before.select(KEY), KEY, join_type='left anti')

    return {
        'version': new_table.version(),
        'filas_v0': old.count_rows(),
        'filas_actual': new.count_rows(),
        'cambiadas': changed.sort_by([(c, 'ascending') for c in KEY]),
        'insertadas': inserted.num_rows,
    }


# =============================================================================
def _markdown_table(header: list[str], rows: list[list]) -> list[str]:
    """Líneas de una tabla Markdown."""
    return [
        '| ' + ' | '.join(header) + ' |',
        '|' + '---|' * len(header),
        *('| ' + ' | '.join(str(v) for v in row) + ' |' for row in rows),
    ]


# =============================================================================
def render(target: Path, history: list[dict], result: dict,
           year: int, month: int) -> str:
    """Arma el informe en Markdown con el historial y la comparación.

    Args:
        target (Path): Tabla Delta.
        history (list[dict]): Salida de `history_rows`.
        result (dict): Salida de `compare`.
        year (int): Año de la partición comparada.
        month (int): Mes de la partición comparada.

    Returns:
        str: Contenido del informe.
    """
    changed: pa.Table = result['cambiadas']
    version = result['version']
    diffs = pc.subtract(changed['tip_actual'], changed['tip_v0'])
    unique_diffs = sorted(pc.unique(pc.round(diffs, 2)).to_pylist())

    lines = [
        '# Time travel en Delta: versión 0 vs versión actual',
        '',
        f'- Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'- Tabla: `{target}`',
        f'- Partición comparada: `year={year}`, `month={month}`',
        f'- Versión actual: {version}',
        '',
        'Las versiones antiguas se leen con `DeltaTable(ruta, version=0)`. '
        'Cuando VACUUM borre los archivos antiguos, esa lectura dejará de '
        'funcionar; este informe conserva el resultado.',
        '',
        '## Historial (`history()`)',
        '',
        *_markdown_table(
            ['Versión', 'Fecha', 'Operación', 'Métricas'],
            [[h['version'], f'{h["fecha"]:%Y-%m-%d %H:%M:%S}', h['operacion'],
              '<br>'.join(f'{k}: {v:,}' for k, v in h['metricas'].items())]
             for h in history],
        ),
        '',
        '## La versión 0 conserva los datos originales',
        '',
        *_markdown_table(
            ['', 'Versión 0', f'Versión {version}'],
            [['Filas en la tabla', f'{result["filas_v0"]:,}',
              f'{result["filas_actual"]:,}']],
        ),
        '',
        f'- Filas con `tip_amount` distinto: {changed.num_rows:,}',
        f'- Diferencia de `tip_amount` (actual − v0): {unique_diffs}',
        f'- Filas nuevas (clave inexistente en la versión 0): '
        f'{result["insertadas"]:,}',
        '',
        f'Ejemplo de {min(SAMPLE, changed.num_rows)} filas corregidas:',
        '',
        *_markdown_table(
            [*KEY, 'tip_amount v0', f'tip_amount v{version}'],
            [[r[c] for c in KEY] + [r['tip_v0'], r['tip_actual']]
             for r in changed.slice(0, SAMPLE).to_pylist()],
        ),
        '',
    ]

    return '\n'.join(lines)


# =============================================================================
def save_report(text: str, out_dir: Path) -> Path:
    """Guarda el informe como `time_travel_<AAAAMMDD_HHMMSS>.md`.

    Args:
        text (str): Contenido del informe.
        out_dir (Path): Directorio de salida; se crea si no existe.

    Returns:
        Path: Ruta del informe.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'time_travel_{datetime.now():%Y%m%d_%H%M%S}.md'
    path.write_text(text)

    return path


# =============================================================================
def main() -> int:
    """Compara la versión 0 con la actual y guarda el informe.

    Returns:
        int: `0` siempre.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tabla', type=Path, default=TARGET)
    parser.add_argument('--salida', type=Path, default=METRICS_DIR)
    parser.add_argument('--year', type=int, default=2025)
    parser.add_argument('--month', type=int, default=1)
    args = parser.parse_args()

    history = history_rows(args.tabla)
    result = compare(args.tabla, args.year, args.month)
    path = save_report(
        render(args.tabla, history, result, args.year, args.month), args.salida
    )

    print(f'Versiones en el historial: {len(history)}')
    print(f'Filas con tip_amount distinto: {result["cambiadas"].num_rows:,}')
    print(f'Filas nuevas: {result["insertadas"]:,}')
    print(f'Informe -> {path}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
