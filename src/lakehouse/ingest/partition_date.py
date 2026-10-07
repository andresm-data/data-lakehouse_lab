"""Une archivos Parquet o CSV en un dataset particionado por fecha."""
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from lakehouse.ingest._cli import base_parser, run
from lakehouse.ingest._io import (
    DEFAULT,
    common_schema,
    expand_origins,
    remove_target,
    scan,
    write_partitioned,
)
from lakehouse.ingest._validation import validate


# =============================================================================
GRANULARITIES: dict[str, tuple[str, ...]] = {
    'year': ('year',),
    'month': ('year', 'month'),
    'day': ('year', 'month', 'day')
}
DERIVED = {'year': pc.year, 'month': pc.month, 'day': pc.day}


# =============================================================================
def _validate(
    files: list[Path],
    target: Path,
    date_column: str,
    granularity: str,
    start: datetime | None,
    end: datetime | None,
    format_: str,
    compression: str,
    overwrite: bool
) -> pa.Schema:
    """Valida los parámetros del particionado por fecha.

    Aplica a cada archivo de origen las validaciones comunes, usando la
    columna de fecha como columna de partición.

    Args:
        files: Archivos Parquet o CSV de origen.
        target: Directorio raíz del dataset de salida.
        date_column: Columna de tipo fecha o timestamp a particionar.
        granularity: Nivel de partición; una de las claves de
            `GRANULARITIES`.
        start: Límite inferior inclusivo del filtro de fechas, o `None`.
        end: Límite superior exclusivo del filtro de fechas, o `None`.
        format_: Formato de destino ('parquet' o 'csv').
        compression: Códec de compresión ya resuelto para el formato.
        overwrite: Si es `True`, permite escribir sobre un destino existente.

    Returns:
        pa.Schema: Esquema común de los archivos de origen.

    Raises:
        ValueError: Si la granularidad no es válida.
        ValueError: Si `start` no es anterior a `end`.
        ValueError: Si la columna de fecha no es de tipo fecha o timestamp.
        ValueError: Si el origen ya contiene columnas con el nombre de las
            particiones derivadas.
    """
    if granularity not in GRANULARITIES:
        raise ValueError(
            f'Granularidad "{granularity}" no válida. '
            f'Opciones: {", ".join(GRANULARITIES)}'
        )

    if start and end and start >= end:
        raise ValueError(f'El inicio {start} debe ser anterior al fin {end}')

    for f in files:
        validate(f, target, format_, compression, [date_column], overwrite)

    schema = common_schema(files)
    type_ = schema.field(date_column).type

    if not (pa.types.is_timestamp(type_) or pa.types.is_date(type_)):
        raise ValueError(
            f'La columna "{date_column}" es de tipo {type_}; '
            'se requiere fecha o timestamp'
        )

    clashes = [c for c in GRANULARITIES[granularity] if c in schema.names]

    if clashes:
        raise ValueError(f'El origen ya contiene las columnas: {clashes}')

    return schema


# =============================================================================
def _add_date_parts(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    date_column: str,
    start: datetime | None,
    end: datetime | None
) -> Iterator[pa.RecordBatch]:
    """Filtra cada lote por fecha y le agrega las columnas de partición.

    Args:
        batches: Lotes de registros de origen.
        schema: Esquema de salida: el de origen más las particiones.
        date_column: Columna de la que se derivan las particiones.
        start: Límite inferior inclusivo, o `None`.
        end: Límite superior exclusivo, o `None`.

    Yields:
        pa.RecordBatch: Lotes filtrados con el esquema `schema`.
    """
    for batch in batches:
        col = batch.column(date_column)
        conditions = []

        if start:
            conditions.append(pc.greater_equal(col, pa.scalar(start).cast(col.type)))

        if end:
            conditions.append(pc.less(col, pa.scalar(end).cast(col.type)))

        if conditions:
            mask = conditions[0] if len(conditions) == 1 else pc.and_(*conditions)
            batch = batch.filter(mask)
            col = batch.column(date_column)

        parts = [DERIVED[f.name](col) for f in list(schema)[batch.num_columns:]]

        yield pa.RecordBatch.from_arrays([*batch.columns, *parts], schema=schema)


# =============================================================================
def partition_date(
    origins: list[Path],
    target: Path,
    date_column: str,
    granularity: str = 'month',
    start: datetime | None = None,
    end: datetime | None = None,
    format_: str = 'parquet',
    compression: str | None = None,
    overwrite: bool = False
) -> None:
    """Une varios archivos en un único dataset particionado por fecha.

    Deriva de `date_column` las columnas de partición (`year`, `month`,
    `day` según la granularidad) y escribe el resultado en estructura de
    directorios estilo Hive. Cada fila se ubica según su propia fecha, sin
    importar el archivo de origen del que provenga. Los archivos se leen de
    uno en uno y por lotes para limitar el uso de memoria.

    Args:
        origins: Archivos Parquet o CSV de origen, o una lista con un
            único directorio que los contiene.
        target: Directorio raíz del dataset de salida.
        date_column: Columna de tipo fecha o timestamp a particionar.
        granularity (opcional): Nivel de partición: 'year', 'month' o 'day'.
            Por defecto 'month'.
        start (opcional): Descarta las filas anteriores a esta fecha
            (inclusiva). Por defecto `None`.
        end (opcional): Descarta las filas desde esta fecha en adelante
            (exclusiva). Por defecto `None`.
        format_ (opcional): Formato de destino ('parquet' o 'csv').
            Por defecto 'parquet'.
        compression (opcional): Códec de compresión a aplicar. Si es
            `None`, se usa el valor por defecto del formato definido en
            `DEFAULT`. Por defecto `None`.
        overwrite (opcional): Si es `True`, elimina el destino existente
            antes de escribir. Por defecto `False`.
    """
    compression = compression or DEFAULT[format_]
    files, _ = expand_origins(origins)

    schema = _validate(
        files, target, date_column, granularity,
        start, end, format_, compression, overwrite
    )

    if overwrite:
        remove_target(target)

    partition = list(GRANULARITIES[granularity])
    out_schema = schema
    for p in partition:
        out_schema = out_schema.append(pa.field(p, pa.int64()))

    batches = _add_date_parts(
        scan(files, schema), out_schema, date_column, start, end
    )

    write_partitioned(batches, out_schema, target, format_, compression, partition)


# =============================================================================
def main() -> int:
    """Punto de entrada del comando `lh-partition-date`.

    Returns:
        int: Código de salida del proceso: `0` si el particionado fue
            exitoso, `1` si ocurrió un error.
    """
    parser = base_parser(
        __doc__,
        'Archivos Parquet o CSV de entrada, o un directorio que los contiene',
        'Directorio raíz del dataset de salida',
        many=True
    )
    parser.add_argument(
        '--columna',
        required=True,
        metavar='COL',
        help='Columna fecha/timestamp de la que se derivan las particiones'
    )
    parser.add_argument(
        '--granularidad',
        choices=GRANULARITIES,
        default='month',
        help='Nivel de partición (por defecto: month)'
    )
    parser.add_argument(
        '--desde',
        type=datetime.fromisoformat,
        metavar='FECHA',
        help='Fecha mínima inclusiva, ISO 8601 (ej. 2025-01-01)'
    )
    parser.add_argument(
        '--hasta',
        type=datetime.fromisoformat,
        metavar='FECHA',
        help='Fecha máxima exclusiva, ISO 8601 (ej. 2025-04-01)'
    )
    args = parser.parse_args()

    return run(
        partition_date, args.origen, args.destino,
        args.columna, args.granularidad, args.desde, args.hasta,
        args.formato, args.compresion, args.sobrescribir
    )


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
