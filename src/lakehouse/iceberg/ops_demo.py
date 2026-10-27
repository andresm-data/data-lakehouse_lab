"""Demuestra la evolución de partición y el rollback en una tabla Iceberg.

Crea una tabla aparte con el mismo Parquet que `lh-iceberg-create`: carga
todos los meses menos el último con `month()`, cambia la partición a `day()`
y escribe el último mes como dato nuevo. Después repite ese append por error
y vuelve al snapshot anterior.
"""
import argparse
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pyiceberg
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from lakehouse.iceberg.catalog import (
    CATALOG_DB, NAMESPACE, WAREHOUSE, ensure_namespace, get_catalog,
)
from lakehouse.iceberg.create_table import (
    DATE_COLUMN, ORIGIN, PARTITION_NAME, create_table, list_months, load,
)
from lakehouse.iceberg.ops import (
    evolve_partition_spec, files_by_spec, list_snapshots, list_specs,
    rollback_to,
)
from lakehouse.iceberg.snapshots import _location, _markdown_table
from lakehouse.paths import DOCS_DIR


# =============================================================================
IDENTIFIER = f'{NAMESPACE}.yellow_tripdata_ops'
DAY_NAME = 'pickup_day'
REPORT_DIR = DOCS_DIR / 'iceberg'


# =============================================================================
def partitions_by_spec(table: Table) -> list[dict]:
    """Particiones, archivos y filas del snapshot actual por `spec_id`."""
    totals: dict[int, dict] = {}

    for row in table.inspect.partitions().to_pylist():
        entry = totals.setdefault(row['spec_id'], {
            'spec_id': row['spec_id'], 'particiones': 0, 'archivos': 0, 'filas': 0,
        })
        entry['particiones'] += 1
        entry['archivos'] += row['file_count']
        entry['filas'] += row['record_count']

    return [totals[k] for k in sorted(totals)]


# =============================================================================
def range_scan(table: Table, origin: Path, start: date, end: date) -> dict:
    """Consulta un rango de fechas que cruza las dos specs y la valida.

    Returns:
        dict: `filtro`, `filas` (Iceberg), `esperadas` (Parquet de origen) y
            `archivos_por_spec` (archivos planificados por `spec_id`).
    """
    lo = datetime.combine(start, datetime.min.time())
    hi = datetime.combine(end, datetime.min.time())
    scan = table.scan(
        row_filter=And(GreaterThanOrEqual(DATE_COLUMN, lo.isoformat()),
                       LessThan(DATE_COLUMN, hi.isoformat())),
        selected_fields=(DATE_COLUMN,),
    )
    planned: dict[int, int] = {}

    for task in scan.plan_files():
        planned[task.file.spec_id] = planned.get(task.file.spec_id, 0) + 1

    expected = pq.read_table(
        origin, columns=[DATE_COLUMN],
        filters=[(DATE_COLUMN, '>=', lo), (DATE_COLUMN, '<', hi)],
    ).num_rows

    return {
        'filtro': f"{DATE_COLUMN} >= '{start}' AND {DATE_COLUMN} < '{end}'",
        'filas': scan.to_arrow().num_rows,
        'esperadas': expected,
        'archivos_por_spec': dict(sorted(planned.items())),
    }


# =============================================================================
def run(table: Table, origin: Path, start: date, end: date) -> dict:
    """Ejecuta la demo completa sobre una tabla vacía particionada por mes.

    Returns:
        dict: Estado de la tabla en cada paso, para el informe.
    """
    months = list_months(origin)
    old, new = months[:-1], months[-1:]
    result: dict = {'meses_viejos': old, 'mes_nuevo': new[0]}

    # 1. Evolución de partición.
    load(table, origin, old)
    result['antes'] = {'specs': list_specs(table), 'archivos': files_by_spec(table)}

    evolve_partition_spec(table, DATE_COLUMN, DayTransform(), DAY_NAME,
                          remove=PARTITION_NAME)
    load(table, origin, new)
    result['despues'] = {
        'specs': list_specs(table), 'archivos': files_by_spec(table),
        'particiones': partitions_by_spec(table),
    }
    result['consulta'] = range_scan(table, origin, start, end)

    # 2. Rollback.
    good = table.current_snapshot().snapshot_id
    rows_good = table.scan().count()
    print(f'Snapshot guardado: {good} ({rows_good:,} filas)')

    load(table, origin, new)
    bad = table.current_snapshot().snapshot_id
    rows_bad = table.scan().count()
    print(f'Append duplicado:  {bad} ({rows_bad:,} filas)')

    rollback_to(table, good)
    print(f'Rollback -> {table.current_snapshot().snapshot_id} '
          f'({table.scan().count():,} filas)')

    try:
        rollback_to(table, bad)
        error = None

    except ValueError as e:
        error = str(e)

    result['rollback'] = {
        'bueno': good, 'malo': bad, 'filas_bueno': rows_good,
        'filas_malo': rows_bad, 'filas_despues': table.scan().count(),
        'actual': table.current_snapshot().snapshot_id,
        'time_travel_malo': table.scan(snapshot_id=bad).count(),
        'historial': list_snapshots(table),
        'error_adelante': error,
    }

    return result


# =============================================================================
def _spec_table(specs: list[dict]) -> list[str]:
    return _markdown_table(
        ['spec_id', 'Partición', 'Actual'],
        [[s['spec_id'], f'`{s["descripcion"]}`', 'Sí' if s['actual'] else '']
         for s in specs],
    )


def _files_table(files: list[dict]) -> list[str]:
    return _markdown_table(
        ['spec_id', 'Archivos', 'Filas'],
        [[f['spec_id'], f['archivos'], f'{f["filas"]:,}'] for f in files],
    )


# =============================================================================
def render(identifier: str, table: Table, result: dict) -> str:
    """Arma el informe en Markdown con el antes y el después.

    Returns:
        str: Contenido del informe.
    """
    old = ', '.join(f'{y}-{m:02d}' for y, m in result['meses_viejos'])
    new = '{}-{:02d}'.format(*result['mes_nuevo'])
    q, rb = result['consulta'], result['rollback']
    ok = 'coincide' if q['filas'] == q['esperadas'] else 'NO coincide'

    lines = [
        '# Evolución de partición y rollback en Iceberg',
        '',
        f'- Fecha: {datetime.now():%Y-%m-%d %H:%M:%S}',
        f'- Tabla: `{identifier}` (`{_location(table)}`)',
        f'- PyIceberg: {pyiceberg.__version__}',
        '',
        '## 1. Evolución de partición',
        '',
        f'Se cargó {old} con `month()`, se cambió la partición a `day()` '
        f'con `update_spec()` y luego se escribió {new}.',
        '',
        '### Antes (`table.specs()` y `table.inspect.files()`)',
        '',
        *_spec_table(result['antes']['specs']),
        '',
        *_files_table(result['antes']['archivos']),
        '',
        '### Después',
        '',
        *_spec_table(result['despues']['specs']),
        '',
        *_files_table(result['despues']['archivos']),
        '',
        'Particiones por spec (`table.inspect.partitions()`):',
        '',
        *_markdown_table(
            ['spec_id', 'Particiones', 'Archivos', 'Filas'],
            [[p['spec_id'], p['particiones'], p['archivos'], f'{p["filas"]:,}']
             for p in result['despues']['particiones']],
        ),
        '',
        'Los archivos viejos siguen en la spec 0: Iceberg no los reescribió.',
        '',
        '### Consulta que cruza las dos specs',
        '',
        f'Filtro: `{q["filtro"]}`',
        '',
        f'- Filas en Iceberg: {q["filas"]:,}',
        f'- Filas en el Parquet de origen: {q["esperadas"]:,} ({ok})',
        '- Archivos planificados por spec: '
        + ', '.join(f'spec {k}: {v}' for k, v in q['archivos_por_spec'].items()),
        '',
        '## 2. Rollback (`manage_snapshots().rollback_to_snapshot()`)',
        '',
        f'- Snapshot guardado: `{rb["bueno"]}` ({rb["filas_bueno"]:,} filas)',
        f'- Append duplicado de {new}: `{rb["malo"]}` ({rb["filas_malo"]:,} filas)',
        f'- Después del rollback: `{rb["actual"]}` ({rb["filas_despues"]:,} filas)',
        f'- Time travel al snapshot malo: {rb["time_travel_malo"]:,} filas',
        f'- Volver "hacia adelante" al snapshot malo: `{rb["error_adelante"]}`',
        '',
        '### Historial (`table.inspect.snapshots()`)',
        '',
        *_markdown_table(
            ['snapshot_id', 'parent_id', 'Operación', 'Total filas',
             'En línea actual', 'Actual'],
            [[s['snapshot_id'], s['parent_id'] or '—', s['operacion'],
              f'{int(s["resumen"].get("total-records", 0)):,}',
              'Sí' if s['en_linea_actual'] else 'No',
              'Sí' if s['actual'] else '']
             for s in rb['historial']],
        ),
        '',
        'El rollback mueve el puntero de la rama `main`; no crea un snapshot '
        'nuevo. El snapshot malo queda fuera de la línea actual, pero sigue '
        'disponible para time travel hasta que se expire.',
        '',
    ]

    return '\n'.join(lines)


# =============================================================================
def save_report(text: str, out_dir: Path) -> Path:
    """Guarda el informe como `ops_<AAAAMMDD_HHMMSS>.md`."""
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'ops_{datetime.now():%Y%m%d_%H%M%S}.md'
    path.write_text(text)

    return path


# =============================================================================
def main() -> int:
    """Crea la tabla de la demo, ejecuta los pasos y guarda el informe.

    Returns:
        int: `0` siempre.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('origen', type=Path, nargs='?', default=ORIGIN)
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    parser.add_argument('--tabla', default=IDENTIFIER)
    parser.add_argument('--salida', type=Path, default=REPORT_DIR)
    parser.add_argument('--desde', type=date.fromisoformat,
                        default=date(2025, 2, 15))
    parser.add_argument('--hasta', type=date.fromisoformat,
                        default=date(2025, 3, 15))
    args = parser.parse_args()

    catalog = get_catalog(args.catalogo, args.warehouse)
    ensure_namespace(catalog, args.tabla.rsplit('.', 1)[0])
    table = create_table(catalog, args.tabla, pq.read_schema(args.origen))

    result = run(table, args.origen, args.desde, args.hasta)

    for step in ('antes', 'despues'):
        print(f'{step.capitalize()}: specs={[s["spec_id"] for s in result[step]["specs"]]} '
              f'archivos={result[step]["archivos"]}')

    q = result['consulta']
    print(f'Consulta {q["filtro"]}: {q["filas"]:,} filas '
          f'(esperadas {q["esperadas"]:,}), archivos por spec {q["archivos_por_spec"]}')

    path = save_report(render(args.tabla, table, result), args.salida)
    print(f'Informe -> {path}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
