"""Convierte uno o varios Parquet a Parquet o a CSV."""
import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq


# =============================================================================
AVAILABLE_COMPRESSIONS: dict[str, tuple[str, ...]] = {
    'parquet': ('none', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'),
    'csv': ('none', 'gzip', 'bz2', 'lz4', 'zstd')
}
DEFAULT = {'parquet': 'zstd', 'csv': 'none'}
GRANULARITIES: dict[str, tuple[str, ...]] = {
    'year': ('year',),
    'month': ('year', 'month'),
    'day': ('year', 'month', 'day')
}


# =============================================================================
def _validate(
    origin: Path,
    target: Path,
    format_: str,
    compression: str,
    partition: list[str] | None,
    overwrite: bool
) -> None:
    """Valida los parámetros de entrada del proceso.

    Args:
        origin: Ruta del archivo Parquet de origen.
        target: Ruta de salida; archivo o directorio si se particiona.
        format_: Formato de destino ('parquet' o 'csv').
        compression: Códec de compresión ya resuelto para el formato.
        partition: Columnas de partición estilo Hive, o `None` para
            generar un único archivo.
        overwrite (bool): Si es `True`, permite escribir sobre un destino
            existente.

    Raises:
        ValueError: Si la compresión no es válida para el formato indicado.
        ValueError: Si el códec no está disponible en el build de `pyarrow`.
        ValueError: Si se solicita CSV particionado con compresión.
        FileNotFoundError: Si el archivo de origen no existe.
        FileExistsError: Si el destino ya existe y no se permite
            sobrescribir.
        ValueError: Si el destino coincide con el origen o lo contiene.
        ValueError: Si alguna columna de partición no existe en el esquema
            del origen.
    """
    # Compresión no disponible
    if compression not in AVAILABLE_COMPRESSIONS[format_]:
        raise ValueError(
            f'Compresión "{compression}" no válida para {format_}. '
            f'Opciones: {", ".join(AVAILABLE_COMPRESSIONS[format_])}'
        )

    elif compression != 'none' and not pa.Codec.is_available(compression):
        raise ValueError(
            f'Este build de pyarrow no incluye el códec "{compression}"'
        )

    # Formatos no disponibles
    if format_ == 'csv' and partition and compression != 'none':
        raise ValueError(
            'CSV particionado no admite compresión con pyarrow; use -c none'
        )

    # Rutas
    if not origin.is_file():
        raise FileNotFoundError(f'No existe el archivo de origen: {origin}')

    if target.exists() and not overwrite:
        raise FileExistsError(f'{target} ya existe (use --sobrescribir)')

    if target.resolve() in (origin.resolve(), *origin.resolve().parents):
        raise ValueError(
            f'El destino {target} contiene o coincide con el origen {origin}'
        )

    if partition:
        missing = [
            c for c in partition if c not in pq.read_schema(origin).names
        ]

        if missing:
            raise ValueError(f'Columnas inexistentes: {missing}')


# =============================================================================
def convert(
    origin: Path,
    target: Path,
    format_: str = 'parquet',
    compression: str | None = None,
    partition: list[str] | None = None,
    overwrite: bool = False
) -> None:
    """Realiza la conversión de formato de acuerdo a los parámetros
    proporcionados.

    La lectura se realiza por lotes para limitar el uso de memoria. Si se
    indican columnas de partición, la salida es un dataset en estructura de
    directorios estilo Hive; en caso contrario, se genera un único archivo.

    Args:
        origin: Ruta del archivo Parquet de origen.
        target: Ruta del archivo de salida, o directorio raíz del
            dataset cuando se especifica `partition`.
        format_ (opcional): Formato de destino ('parquet' o 'csv').
            Por defecto to 'parquet'.
        compression (opcional): Códec de compresión a aplicar.
            Si es `None`, se usa el valor por defecto del formato definido en
            `DEFAULT`.
            Por defecto to `None`.
        partition (opcional): Columnas por las que se particiona la salida.
            Por defecto to `None`.
        overwrite (bool, optional): Si es `True`, elimina el destino
            existente (archivo o directorio completo) antes de escribir.
            Por defecto to `False`.
    """
    # Verifica si se proporciona aguna comprensión
    compression = compression or DEFAULT[format_]

    # Valida los parámetros del proceso
    _validate(
        origin, target, format_, compression, partition, overwrite
    )

    # Elimina el destino previo para no mezclar datos antiguos con los nuevos
    if overwrite:
        _remove_target(target)

    # Particionado por lotes
    if partition:
        dataset = ds.dataset(origin, format='parquet')
        fmt = ds.ParquetFileFormat() if format_ == 'parquet' else ds.CsvFileFormat()
        options = (
            fmt.make_write_options(compression=compression)
            if format_ == 'parquet'
            else None
        )

        ds.write_dataset(
            dataset,
            target,
            format=fmt,
            file_options=options,
            partitioning=partition,
            partitioning_flavor='hive',
            existing_data_behavior='overwrite_or_ignore'
        )
        return

    # Archivo único, en streaming por lotes
    target.parent.mkdir(parents=True, exist_ok=True)
    pf = pq.ParquetFile(origin)
    batchs = pf.iter_batches()

    if format_ == 'parquet':
        with pq.ParquetWriter(target, pf.schema_arrow, compression=compression) as w:
            for b in batchs:
                w.write_batch(b)

    elif compression == 'none':
        with pacsv.CSVWriter(target, pf.schema_arrow) as w:
            for b in batchs:
                w.write_batch(b)
    else:
        with pa.CompressedOutputStream(str(target), compression) as out, \
                pacsv.CSVWriter(out, pf.schema_arrow) as w:
            for b in batchs:
                w.write_batch(b)


# =============================================================================
def _remove_target(target: Path) -> None:
    """Elimina el destino si existe, ya sea un archivo o un directorio.

    Args:
        target: Ruta del archivo o directorio a eliminar.
    """
    if target.is_dir():
        shutil.rmtree(target)

    elif target.exists():
        target.unlink()


# =============================================================================
def _expand_origins(origins: list[Path]) -> list[Path]:
    """Expande los directorios de origen a los archivos Parquet que contienen.

    Args:
        origins: Archivos Parquet o directorios que los contienen.

    Returns:
        list[Path]: Archivos Parquet de origen, en el orden recibido; los de
            cada directorio se ordenan por nombre.

    Raises:
        FileNotFoundError: Si no se encuentra ningún archivo Parquet.
    """
    files = []

    for origin in origins:
        if origin.is_dir():
            files.extend(sorted(origin.glob('*.parquet')))

        else:
            files.append(origin)

    if not files:
        raise FileNotFoundError(
            f'No se encontraron archivos Parquet en: {origins}'
        )

    return files


# =============================================================================
def _validate_by_date(
    origins: list[Path],
    target: Path,
    date_column: str,
    granularity: str,
    start: datetime | None,
    end: datetime | None,
    format_: str,
    compression: str,
    overwrite: bool
) -> None:
    """Valida los parámetros del particionado por fecha.

    Aplica a cada archivo de origen las validaciones de `_validate`, usando
    la columna de fecha como columna de partición.

    Args:
        origins: Archivos Parquet de origen.
        target: Directorio raíz del dataset de salida.
        date_column: Columna de tipo fecha o timestamp a particionar.
        granularity: Nivel de partición; una de las claves de
            `GRANULARITIES`.
        start: Límite inferior inclusivo del filtro de fechas, o `None`.
        end: Límite superior exclusivo del filtro de fechas, o `None`.
        format_: Formato de destino ('parquet' o 'csv').
        compression: Códec de compresión ya resuelto para el formato.
        overwrite: Si es `True`, permite escribir sobre un destino existente.

    Raises:
        ValueError: Si la granularidad no es válida.
        ValueError: Si `start` no es anterior a `end`.
        ValueError: Si los archivos de origen no comparten el mismo esquema.
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

    for origin in origins:
        _validate(
            origin, target, format_, compression, [date_column], overwrite
        )

    # Esquema común a todos los archivos
    schema = pq.read_schema(origins[0])
    different = [
        o for o in origins[1:]
        if not pq.read_schema(o).equals(schema, check_metadata=False)
    ]

    if different:
        raise ValueError(
            f'Esquema distinto al de {origins[0]}: {[str(o) for o in different]}'
        )

    # Columna de fecha y columnas derivadas
    type_ = schema.field(date_column).type

    if not (pa.types.is_timestamp(type_) or pa.types.is_date(type_)):
        raise ValueError(
            f'La columna "{date_column}" es de tipo {type_}; '
            'se requiere fecha o timestamp'
        )

    clashes = [c for c in GRANULARITIES[granularity] if c in schema.names]

    if clashes:
        raise ValueError(f'El origen ya contiene las columnas: {clashes}')


# =============================================================================
def convert_by_date(
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
    """Une varios Parquet en un único dataset particionado por fecha.

    Deriva de `date_column` las columnas de partición (`year`, `month`, `day`
    según la granularidad) y escribe el resultado en estructura de
    directorios estilo Hive. Cada fila se ubica según su propia fecha, sin
    importar el archivo de origen del que provenga. La lectura se realiza
    por lotes para limitar el uso de memoria.

    Args:
        origins: Archivos Parquet de origen o directorios que los contienen.
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
    origins = _expand_origins(origins)

    _validate_by_date(
        origins, target, date_column, granularity,
        start, end, format_, compression, overwrite
    )

    if overwrite:
        _remove_target(target)

    dataset = ds.dataset([str(o) for o in origins], format='parquet')

    # Columnas originales más las particiones derivadas de la fecha
    field = ds.field(date_column)
    derived = {'year': pc.year(field), 'month': pc.month(
        field), 'day': pc.day(field)}
    partition = list(GRANULARITIES[granularity])
    columns = {name: ds.field(name) for name in dataset.schema.names}
    columns.update({p: derived[p] for p in partition})

    # Filtro opcional por rango de fechas
    type_ = dataset.schema.field(date_column).type
    filter_ = None

    if start:
        filter_ = field >= pa.scalar(start).cast(type_)

    if end:
        cond = field < pa.scalar(end).cast(type_)
        filter_ = cond if filter_ is None else filter_ & cond

    fmt = ds.ParquetFileFormat() if format_ == 'parquet' else ds.CsvFileFormat()
    options = (
        fmt.make_write_options(compression=compression)
        if format_ == 'parquet'
        else None
    )

    ds.write_dataset(
        dataset.scanner(columns=columns, filter=filter_),
        target,
        format=fmt,
        file_options=options,
        partitioning=partition,
        partitioning_flavor='hive',
        existing_data_behavior='overwrite_or_ignore'
    )


# =============================================================================
def main() -> int:
    """Punto de entrada de la línea de comandos.

    Interpreta los argumentos, ejecuta la conversión y reporta el resultado
    por consola. Los errores de validación, de E/S y de pyarrow se muestran
    en stderr en lugar de propagarse.

    Returns:
        int: Código de salida del proceso: `0` si la conversión fue exitosa,
            `1` si ocurrió un error.
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        'origen',
        type=Path,
        nargs='+',
        help='Archivo Parquet de entrada; varios archivos o directorios '
        'solo con --particion-fecha'
    )
    parser.add_argument(
        'destino',
        type=Path,
        help='Archivo de salida, o directorio si se usa --particiones'
    )
    parser.add_argument(
        '-f', '--formato', choices=AVAILABLE_COMPRESSIONS, default='parquet'
    )
    parser.add_argument(
        '-c',
        '--compresion',
        help='Parquet: ' +
        '/'.join(AVAILABLE_COMPRESSIONS['parquet']) +
        ' | CSV: ' + '/'.join(AVAILABLE_COMPRESSIONS['csv'])
    )
    partitions = parser.add_mutually_exclusive_group()
    partitions.add_argument(
        '-p',
        '--particiones',
        nargs='+',
        metavar='COL',
        help='Columnas de partición (estilo Hive)'
    )
    partitions.add_argument(
        '--particion-fecha',
        metavar='COL',
        help='Columna fecha/timestamp de la que se derivan las particiones'
    )
    parser.add_argument(
        '--granularidad',
        choices=GRANULARITIES,
        help='Nivel de partición por fecha (por defecto: month)'
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
    parser.add_argument('--sobrescribir', action='store_true')

    args = parser.parse_args()

    if not args.particion_fecha:
        if args.granularidad or args.desde or args.hasta:
            parser.error(
                '--granularidad, --desde y --hasta requieren --particion-fecha'
            )

        if len(args.origen) > 1 or args.origen[0].is_dir():
            parser.error(
                'varios orígenes o un directorio requieren --particion-fecha'
            )

    try:
        if args.particion_fecha:
            convert_by_date(
                args.origen, args.destino,
                args.particion_fecha, args.granularidad or 'month',
                args.desde, args.hasta,
                args.formato, args.compresion, args.sobrescribir
            )

        else:
            convert(
                args.origen[0], args.destino,
                args.formato, args.compresion,
                args.particiones, args.sobrescribir
            )

    except (ValueError, OSError, pa.ArrowException) as e:
        print(f'Error: {e}', file=sys.stderr)

        return 1

    print(f'Listo -> {args.destino}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
