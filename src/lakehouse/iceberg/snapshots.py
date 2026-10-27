"""Muestra los snapshots de la tabla Iceberg y prueba el hidden partitioning."""
import argparse
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table

from lakehouse.iceberg.catalog import CATALOG_DB, WAREHOUSE, get_catalog
from lakehouse.iceberg.create_table import DATE_COLUMN, IDENTIFIER, describe_spec
from lakehouse.paths import DOCS_DIR


# =============================================================================
REPORT_DIR = DOCS_DIR / 'iceberg'
SUMMARY = ['added-records', 'total-records', 'added-data-files', 'source-month']


# =============================================================================
def snapshot_rows(table: Table) -> list[dict]:
    """Resume los snapshots de la tabla, del más antiguo al actual.

    Args:
        table (Table): Tabla Iceberg.

    Returns:
        list[dict]: Una fila por snapshot con `snapshot_id`, `parent_id`,
            `fecha`, `operacion`, `actual` (si es el snapshot vigente) y
            `resumen` (solo las claves de `SUMMARY`).
    """
    current = table.current_snapshot()
    current_id = current.snapshot_id if current else None
    rows = []

    for row in table.inspect.snapshots().sort_by('committed_at').to_pylist():
        summary = dict(row['summary'] or [])
        rows.append({
            'snapshot_id': row['snapshot_id'],
            'parent_id': row['parent_id'],
            'fecha': row['committed_at'],
            'operacion': row['operation'],
            'actual': row['snapshot_id'] == current_id,
            'resumen': {k: summary[k] for k in SUMMARY if k in summary},
        })

    return rows


# =============================================================================
def partition_rows(table: Table) -> list[dict]:
    """Filas y archivos de cada partición del snapshot actual.

    Returns:
        list[dict]: `particion` (valor de `month(...)` como `AAAA-MM`),
            `filas` y `archivos`, ordenadas por partición.
    """
    rows = []
    field = table.spec().fields[0].name

    for row in table.inspect.partitions().to_pylist():
        months = row['partition'][field]
        rows.append({
            'particion': f'{1970 + months // 12}-{months % 12 + 1:02d}',
            'filas': row['record_count'],
            'archivos': row['file_count'],
        })

    return sorted(rows, key=lambda r: r['particion'])


# =============================================================================
def month_scan(table: Table, start: date, end: date) -> dict:
    """Filtra por la columna de fecha original y mide la poda de archivos.

    El filtro usa solo `tpep_pickup_datetime`; Iceberg lo traduce a la
    partición `month(...)` y descarta los archivos de otros meses sin
    abrirlos.

    Args:
        table (Table): Tabla Iceberg.
        start (date): Inicio del rango (incluido).
        end (date): Fin del rango (excluido).

    Returns:
        dict: `filtro` (texto), `archivos_leidos`, `archivos_totales` y
            `filas` del rango.
    """
    row_filter = And(
        GreaterThanOrEqual(DATE_COLUMN, f'{start.isoformat()}T00:00:00'),
        LessThan(DATE_COLUMN, f'{end.isoformat()}T00:00:00'),
    )
    scan = table.scan(row_filter=row_filter, selected_fields=(DATE_COLUMN,))

    return {
        'filtro': f"{DATE_COLUMN} >= '{start}' AND {DATE_COLUMN} < '{end}'",
        'archivos_leidos': len(list(scan.plan_files())),
        'archivos_totales': len(list(table.scan().plan_files())),
        'filas': scan.to_arrow().num_rows,
    }


# =============================================================================
def _location(table: Table) -> str:
    """Ubicación de la tabla, relativa al directorio actual si es posible."""
    path = Path(urlparse(table.location()).path)

    try:
        return str(path.relative_to(Path.cwd()))

    except ValueError:
        return str(path)


# =============================================================================
def _markdown_table(header: list[str], rows: list[list]) -> list[str]:
    """Líneas de una tabla Markdown."""
    return [
        '| ' + ' | '.join(header) + ' |',
        '|' + '---|' * len(header),
        *('| ' + ' | '.join(str(v) for v in row) + ' |' for row in rows),
    ]


# =============================================================================
def render(identifier: str, table: Table, snapshots: list[dict],
           partitions: list[dict], scan: dict) -> str:
    """Arma el informe en Markdown con snapshots, particiones y la consulta.

    Returns:
        str: Contenido del informe.
    """
    lines = [
        '# Snapshots y hidden partitioning en Iceberg',
        '',
        f'- Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'- Tabla: `{identifier}` (`{_location(table)}`)',
        f'- Partición: `{describe_spec(table)}`',
        '',
        '## Snapshots (`table.inspect.snapshots()`)',
        '',
        *_markdown_table(
            ['snapshot_id', 'parent_id', 'committed_at (UTC)', 'Operación',
             'Resumen', 'Actual'],
            [[s['snapshot_id'], s['parent_id'] or '—',
              f'{s["fecha"]:%Y-%m-%d %H:%M:%S}', s['operacion'],
              '<br>'.join(f'{k}: {v}' for k, v in s['resumen'].items()),
              'Sí' if s['actual'] else '']
             for s in snapshots],
        ),
        '',
        '## Particiones del snapshot actual (`table.inspect.partitions()`)',
        '',
        *_markdown_table(
            ['Partición', 'Filas', 'Archivos'],
            [[p['particion'], f'{p["filas"]:,}', p['archivos']]
             for p in partitions],
        ),
        '',
        '## Consulta por la columna de fecha original',
        '',
        f'Filtro: `{scan["filtro"]}`',
        '',
        f'- Archivos planificados: {scan["archivos_leidos"]} de '
        f'{scan["archivos_totales"]}',
        f'- Filas devueltas: {scan["filas"]:,}',
        '',
        'El filtro no menciona ninguna columna de partición: Iceberg aplica '
        '`month()` al rango y descarta los archivos de los otros meses.',
        '',
    ]

    return '\n'.join(lines)


# =============================================================================
def save_report(text: str, out_dir: Path) -> Path:
    """Guarda el informe como `snapshots_<AAAAMMDD_HHMMSS>.md`."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'snapshots_{datetime.now():%Y%m%d_%H%M%S}.md'
    path.write_text(text)

    return path


# =============================================================================
def main() -> int:
    """Muestra los snapshots, las particiones y guarda el informe.

    Returns:
        int: `0` siempre.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    parser.add_argument('--tabla', default=IDENTIFIER)
    parser.add_argument('--salida', type=Path, default=REPORT_DIR)
    parser.add_argument('--desde', type=date.fromisoformat,
                        default=date(2025, 2, 1))
    parser.add_argument('--hasta', type=date.fromisoformat,
                        default=date(2025, 3, 1))
    args = parser.parse_args()

    table = get_catalog(args.catalogo, args.warehouse).load_table(args.tabla)
    snapshots = snapshot_rows(table)
    partitions = partition_rows(table)
    scan = month_scan(table, args.desde, args.hasta)

    print('Snapshots:')

    for s in snapshots:
        mark = ' (actual)' if s['actual'] else ''
        print(f'  {s["snapshot_id"]}  {s["fecha"]:%Y-%m-%d %H:%M:%S} UTC  '
              f'{s["operacion"]}  {s["resumen"]}{mark}')

    print(f'Consulta {scan["filtro"]}: {scan["filas"]:,} filas, '
          f'{scan["archivos_leidos"]} de {scan["archivos_totales"]} archivos')

    path = save_report(
        render(args.tabla, table, snapshots, partitions, scan), args.salida
    )
    print(f'Informe -> {path}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
