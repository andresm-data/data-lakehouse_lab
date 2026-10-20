"""Ejecuta VACUUM sobre la tabla Delta y verifica qué archivos se borraron."""
import argparse
import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable

from lakehouse.delta.merge_table import METRICS_DIR, TARGET
from lakehouse.delta.time_travel import _markdown_table


# =============================================================================
DEFAULT_RETENTION = 168
MB = 1024 * 1024


# =============================================================================
def files_on_disk(target: Path) -> set[str]:
    """Archivos Parquet de datos que hay en disco, con ruta relativa a la tabla.

    Args:
        target (Path): Tabla Delta.

    Returns:
        set[str]: Rutas como `year=2025/month=1/part-...parquet`; no incluye
            `_delta_log`.
    """
    return {
        str(p.relative_to(target)) for p in target.rglob('*.parquet')
        if '_delta_log' not in p.parts
    }


# =============================================================================
def removed_files(target: Path) -> dict[str, dict]:
    """Lee del log qué archivos se retiraron, en qué versión y cuándo.

    Recorre los commits JSON de `_delta_log` y guarda cada acción `remove`.
    VACUUM decide si borra un archivo según su `deletionTimestamp`.

    Args:
        target (Path): Tabla Delta.

    Returns:
        dict[str, dict]: Por ruta, `version` y `operacion` del commit que lo
            retiró, `fecha` de retiro y `bytes`.
    """
    removed = {}

    for log in sorted((target / '_delta_log').glob('*.json')):
        actions = [json.loads(line) for line in log.read_text().splitlines()
                   if line.strip()]
        operation = next((a['commitInfo'].get('operation') for a in actions
                          if 'commitInfo' in a), None)

        for action in actions:
            if 'remove' not in action:
                continue

            remove = action['remove']
            removed[remove['path']] = {
                'version': int(log.stem),
                'operacion': operation,
                'fecha': datetime.fromtimestamp(
                    remove['deletionTimestamp'] / 1000
                ),
                'bytes': remove.get('size'),
            }

    return removed


# =============================================================================
def vacuum(target: Path, retention_hours: int, dry_run: bool) -> list[str]:
    """Ejecuta VACUUM, o solo lista lo que borraría si `dry_run` es `True`.

    Delta no permite una retención menor a 168 h (7 días) salvo que se
    desactive la comprobación; aquí se desactiva solo en ese caso, porque
    una retención corta rompe el time travel y puede borrar archivos que
    otro proceso aún está leyendo.

    Args:
        target (Path): Tabla Delta.
        retention_hours (int): Horas que se conservan los archivos retirados.
        dry_run (bool): Si es `True`, no borra nada.

    Returns:
        list[str]: Rutas que se borran (o se borrarían), relativas a la tabla.
    """
    paths = DeltaTable(target).vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
        enforce_retention_duration=retention_hours >= DEFAULT_RETENTION,
    )

    return sorted(paths)


# =============================================================================
def can_read_version(target: Path, version: int) -> bool:
    """Indica si la versión pedida aún se puede leer completa (time travel)."""
    try:
        DeltaTable(target, version=version).to_pyarrow_dataset().count_rows()

    except Exception:
        return False

    return True


# =============================================================================
def _mb(value: int | None) -> str:
    """Bytes como texto en MB con 2 decimales."""
    return '—' if value is None else f'{value / MB:,.2f} MB'


# =============================================================================
def render(target: Path, retention_hours: int, result: dict) -> str:
    """Arma el informe en Markdown con el dry run y el VACUUM real.

    Args:
        target (Path): Tabla Delta.
        retention_hours (int): Retención usada en el VACUUM real.
        result (dict): Resultado que arma `main`.

    Returns:
        str: Contenido del informe.
    """
    now = result['fecha']
    removed = result['retirados']
    deleted = result['borrados']
    default_run = set(result['dry_run_defecto'])
    actual_run = set(result['dry_run'])

    rows = []

    for path in sorted(removed, key=lambda p: removed[p]['version']):
        info = removed[path]
        age = (now - info['fecha']).total_seconds() / 3600
        rows.append([
            f'`{path}`', _mb(info['bytes']),
            f'v{info["version"]} ({info["operacion"]})',
            f'{info["fecha"]:%Y-%m-%d %H:%M}', f'{age:,.1f} h',
            'Sí' if path in default_run else 'No',
            'Sí' if path in actual_run else 'No',
            'Sí' if path in deleted else 'No',
        ])

    lines = [
        '# VACUUM en Delta: archivos eliminados',
        '',
        f'- Fecha: {now:%Y-%m-%d %H:%M:%S}',
        f'- Tabla: `{target}`',
        f'- Retención usada: {retention_hours} h '
        f'(por defecto Delta usa {DEFAULT_RETENTION} h = 7 días)',
        '- Comando: `DeltaTable(ruta).vacuum(retention_hours, dry_run=...)`',
    ]

    if retention_hours < DEFAULT_RETENTION:
        lines.append(
            '- **Aviso:** retención menor a 7 días con '
            '`enforce_retention_duration=False`. Solo es aceptable en un '
            'entorno de práctica: rompe el time travel a versiones antiguas.'
        )

    lines += [
        '',
        '## Resumen',
        '',
        *_markdown_table(
            ['Medida', 'Valor'],
            [
                ['Parquet en disco antes', result['disco_antes']],
                ['Archivos activos (versión actual)', result['activos']],
                ['Archivos retirados en el log', len(removed)],
                [f'Dry run con {DEFAULT_RETENTION} h',
                 len(result['dry_run_defecto'])],
                [f'Dry run con {retention_hours} h', len(result['dry_run'])],
                ['Borrados por VACUUM', len(deleted)],
                ['Parquet en disco después', result['disco_despues']],
                ['Espacio liberado', _mb(result['bytes_liberados'])],
                ['¿Se puede leer la versión 0?',
                 'Sí' if result['lee_v0'] else 'No'],
            ],
        ),
        '',
        '## Archivos retirados y por qué se borran o no',
        '',
        'Un archivo es candidato si una versión lo retiró (`remove` en el '
        'log) hace más horas que la retención. Los archivos activos nunca se '
        'borran.',
        '',
        *_markdown_table(
            ['Archivo', 'Tamaño', 'Retirado en', 'Fecha de retiro',
             'Antigüedad', f'Borra con {DEFAULT_RETENTION} h',
             f'Borra con {retention_hours} h', 'Borrado'],
            rows,
        ),
        '',
        '## Verificación',
        '',
        f'- Lo borrado coincide con el dry run: '
        f'{"Sí" if set(deleted) == actual_run else "No"}',
        f'- Lo borrado coincide con lo que desapareció del disco: '
        f'{"Sí" if result["coincide_disco"] else "No"}',
        f'- Archivos activos intactos: '
        f'{"Sí" if result["activos_intactos"] else "No"}',
        '',
    ]

    return '\n'.join(lines)


# =============================================================================
def save_report(text: str, out_dir: Path) -> Path:
    """Guarda el informe como `vacuum_<AAAAMMDD_HHMMSS>.md`.

    Args:
        text (str): Contenido del informe.
        out_dir (Path): Directorio de salida; se crea si no existe.

    Returns:
        Path: Ruta del informe.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f'vacuum_{datetime.now():%Y%m%d_%H%M%S}.md'
    path.write_text(text)

    return path


# =============================================================================
def main() -> int:
    """Hace el dry run, ejecuta VACUUM, verifica el disco y guarda el informe.

    Returns:
        int: `0` si lo borrado coincide con el dry run y con el disco, y los
            archivos activos siguen en disco; `1` en otro caso.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tabla', type=Path, default=TARGET)
    parser.add_argument('--salida', type=Path, default=METRICS_DIR)
    parser.add_argument('--retencion-horas', type=int,
                        default=DEFAULT_RETENTION,
                        help='horas que se conservan los archivos retirados; '
                             'menos de 168 solo en un entorno de práctica')
    args = parser.parse_args()

    table = DeltaTable(args.tabla)
    active = set(pa.table(table.get_add_actions(flatten=True))
                 .to_pydict()['path'])
    removed = removed_files(args.tabla)
    disk_before = files_on_disk(args.tabla)

    default_run = vacuum(args.tabla, DEFAULT_RETENTION, dry_run=True)
    planned = vacuum(args.tabla, args.retencion_horas, dry_run=True)
    deleted = vacuum(args.tabla, args.retencion_horas, dry_run=False)
    disk_after = files_on_disk(args.tabla)

    freed = sum(removed.get(p, {}).get('bytes') or 0 for p in deleted)

    result = {
        'fecha': datetime.now(),
        'retirados': removed,
        'activos': len(active),
        'disco_antes': len(disk_before),
        'disco_despues': len(disk_after),
        'dry_run_defecto': default_run,
        'dry_run': planned,
        'borrados': deleted,
        'bytes_liberados': freed,
        'coincide_disco': disk_before - disk_after == set(deleted),
        'activos_intactos': active <= disk_after,
        'lee_v0': can_read_version(args.tabla, 0),
    }
    path = save_report(render(args.tabla, args.retencion_horas, result),
                       args.salida)

    print(f'Dry run con {DEFAULT_RETENTION} h: {len(default_run)} archivos')
    print(f'Dry run con {args.retencion_horas} h: {len(planned)} archivos')
    print(f'Borrados: {len(deleted)} ({_mb(freed)})')
    print(f'Parquet en disco: {len(disk_before)} -> {len(disk_after)}')
    print(f'Informe -> {path}')

    ok = (set(deleted) == set(planned) and result['coincide_disco']
          and result['activos_intactos'])

    return 0 if ok else 1


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
