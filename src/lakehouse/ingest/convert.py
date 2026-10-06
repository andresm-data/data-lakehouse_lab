"""Convierte un Parquet a Parquet o a CSV."""
import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as ds
import pyarrow.parquet as pq


# =============================================================================
AVAILABLE_COMPRESSIONS: dict[str, tuple[str, ...]] = {
    'parquet': ('none', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'),
    'csv': ('none', 'gzip', 'bz2', 'lz4', 'zstd')
}
DEFAULT = {'parquet': 'zstd', 'csv': 'none'}


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
        overwrite (bool, optional): Si es `True`, permite escribir sobre un
            destino existente.
            Por defecto to `False`.
    """
    # Verifica si se proporciona aguna comprensión
    compression = compression or DEFAULT[format_]

    # Valida los parámetros del proceso
    _validate(
        origin, target, format_, compression, partition, overwrite
    )

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
        'origen', type=Path, help='Archivo Parquet de entrada'
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
    parser.add_argument(
        '-p',
        '--particiones',
        nargs='+',
        metavar='COL',
        help='Columnas de partición (estilo Hive)'
    )
    parser.add_argument('--sobrescribir', action='store_true')

    args = parser.parse_args()

    try:
        convert(
            args.origen, args.destino,
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
