"""Lectura y escritura por lotes de archivos Parquet y CSV."""
import shutil
from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.compute as pc
import pyarrow.parquet as pq


# =============================================================================
AVAILABLE_COMPRESSIONS: dict[str, tuple[str, ...]] = {
    'parquet': ('none', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'),
    'csv': ('none', 'gzip', 'bz2', 'lz4', 'zstd')
}
DEFAULT = {'parquet': 'zstd', 'csv': 'none'}
HIVE_NULL = '__HIVE_DEFAULT_PARTITION__'
CSV_SUFFIXES = ('.csv', '.csv.gz', '.csv.bz2', '.csv.lz4', '.csv.zst')


# =============================================================================
def input_format(path: Path) -> str:
    """Determina el formato de un archivo de entrada por su extensión.

    Args:
        path: Ruta del archivo.

    Returns:
        str: 'parquet' o 'csv'.

    Raises:
        ValueError: Si la extensión no corresponde a un formato soportado.
    """
    name = path.name.lower()

    if name.endswith('.parquet'):
        return 'parquet'

    if name.endswith(CSV_SUFFIXES):
        return 'csv'

    raise ValueError(
        f'Formato de entrada no soportado: {path} '
        f'(use .parquet o {", ".join(CSV_SUFFIXES)})'
    )


# =============================================================================
def _open_csv(path: Path, schema: pa.Schema | None = None) -> pacsv.CSVStreamingReader:
    """Abre un CSV, comprimido o no, como lector por lotes.

    Args:
        path: Ruta del archivo CSV; la compresión se deduce de la extensión.
        schema: Tipos a forzar en las columnas, o `None` para inferirlos.

    Returns:
        pacsv.CSVStreamingReader: Lector de lotes del archivo.
    """
    options = pacsv.ConvertOptions(
        column_types=dict(zip(schema.names, schema.types)) if schema else None
    )

    return pacsv.open_csv(
        pa.input_stream(str(path), compression='detect'),
        convert_options=options
    )


# =============================================================================
def read_schema(path: Path) -> pa.Schema:
    """Obtiene el esquema de un archivo Parquet o CSV.

    En un CSV, los tipos se infieren a partir del primer bloque del archivo.

    Args:
        path: Ruta del archivo.

    Returns:
        pa.Schema: Esquema del archivo.
    """
    if input_format(path) == 'parquet':
        return pq.read_schema(path)

    with _open_csv(path) as reader:
        return reader.schema


# =============================================================================
def expand_origins(origins: list[Path]) -> tuple[list[Path], Path | None]:
    """Expande un directorio de origen a los archivos soportados que contiene.

    Args:
        origins: Archivos de origen, o una lista con un único directorio que
            se recorre de forma recursiva.

    Returns:
        tuple[list[Path], Path | None]: Archivos de origen y, si se recibió
            un directorio, el directorio base.

    Raises:
        ValueError: Si se combina un directorio con otros orígenes.
        FileNotFoundError: Si el directorio no contiene archivos soportados.
    """
    if not any(o.is_dir() for o in origins):
        return list(origins), None

    if len(origins) > 1:
        raise ValueError(
            'Un directorio de origen no puede combinarse con otros orígenes'
        )

    base_dir = origins[0]
    files = sorted(
        p for p in base_dir.rglob('*')
        if p.is_file() and p.name.lower().endswith(('.parquet', *CSV_SUFFIXES))
    )

    if not files:
        raise FileNotFoundError(f'No se encontraron archivos en: {base_dir}')

    return files, base_dir


# =============================================================================
def _partitions(path: Path, base_dir: Path) -> dict[str, str]:
    """Extrae las particiones estilo Hive (`clave=valor`) de una ruta."""
    parts = path.parent.relative_to(base_dir).parts
    return dict(p.split('=', 1) for p in parts if '=' in p)


# =============================================================================
WIDER_TYPES = (pa.null(), pa.int64(), pa.float64(), pa.string())


def _fits(column: pa.Array, type_: pa.DataType) -> bool:
    """Indica si todos los valores de una columna de texto admiten un tipo."""
    if type_ == pa.null():
        return column.null_count == len(column)

    try:
        pc.cast(column, type_)

    except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
        return False

    return True


# =============================================================================
def _widen_csv_types(files: list[Path], schema: pa.Schema) -> pa.Schema:
    """Amplía los tipos de CSV inferidos con pocas filas.

    El tipo de cada columna se infiere solo con el primer bloque del
    archivo, por lo que una columna vacía o entera al inicio puede tener
    después valores que no encajan (por ejemplo, `0.75` en una columna
    inferida como entera). Este paso recorre todos los archivos, solo para
    las columnas inferidas como nulas o enteras, y amplía su tipo en el
    orden nulo → entero → decimal → texto hasta que admita todos los
    valores.

    Args:
        files: Archivos CSV de origen.
        schema: Esquema inferido del primer archivo.

    Returns:
        pa.Schema: Esquema con los tipos ampliados.
    """
    suspects = {f.name: f.type for f in schema if f.type in WIDER_TYPES[:2]}

    if not suspects:
        return schema

    options = pacsv.ConvertOptions(
        column_types={name: pa.string() for name in suspects},
        include_columns=list(suspects),
        strings_can_be_null=True
    )

    for f in files:
        stream = pa.input_stream(str(f), compression='detect')

        with pacsv.open_csv(stream, convert_options=options) as reader:
            for batch in reader:
                for name, type_ in suspects.items():
                    level = WIDER_TYPES.index(type_)

                    while not _fits(batch.column(name), WIDER_TYPES[level]):
                        level += 1

                    suspects[name] = WIDER_TYPES[level]

    for name, type_ in suspects.items():
        schema = schema.set(schema.get_field_index(name), pa.field(name, type_))

    return schema


# =============================================================================
def common_schema(files: list[Path], base_dir: Path | None = None) -> pa.Schema:
    """Obtiene el esquema común a todos los archivos de origen.

    En Parquet se compara el esquema completo, sin metadatos; en CSV solo
    los nombres de columna, y los tipos inferidos en el primer bloque se
    amplían con `_widen_csv_types` para que admitan todos los valores. Si se indica `base_dir`, se agregan al final las
    columnas de partición estilo Hive presentes en las rutas: enteras si
    todos sus valores son numéricos y de texto en otro caso.

    Args:
        files: Archivos de origen.
        base_dir: Directorio base del dataset particionado, o `None`.

    Returns:
        pa.Schema: Esquema común, incluidas las columnas de partición.

    Raises:
        ValueError: Si se mezclan archivos Parquet y CSV.
        ValueError: Si algún archivo tiene un esquema distinto al del primero.
        ValueError: Si una partición coincide con una columna de los datos.
    """
    if len({input_format(f) for f in files}) > 1:
        raise ValueError('No se pueden mezclar archivos Parquet y CSV')

    schema = read_schema(files[0])
    is_csv = input_format(files[0]) == 'csv'
    different = []

    for f in files[1:]:
        other = read_schema(f)
        same = (
            other.names == schema.names if is_csv
            else other.equals(schema, check_metadata=False)
        )

        if not same:
            different.append(str(f))

    if different:
        raise ValueError(f'Esquema distinto al de {files[0]}: {different}')

    if is_csv:
        schema = _widen_csv_types(files, schema)

    if base_dir:
        values: dict[str, list[str]] = {}

        for f in files:
            for key, value in _partitions(f, base_dir).items():
                values.setdefault(key, []).append(value)

        clashes = [k for k in values if k in schema.names]

        if clashes:
            raise ValueError(
                f'Las particiones {clashes} ya existen como columnas'
            )

        for key, vals in values.items():
            numeric = all(v.lstrip('-').isdigit() for v in vals)
            schema = schema.append(pa.field(key, pa.int32() if numeric else pa.string()))

    return schema.remove_metadata()


# =============================================================================
def scan(
    files: list[Path],
    schema: pa.Schema,
    base_dir: Path | None = None
) -> Iterator[pa.RecordBatch]:
    """Lee los archivos de origen por lotes, uno a la vez.

    Leer un archivo a la vez y sin lectura anticipada mantiene acotado el
    uso de memoria, sin importar el tamaño total de los datos.

    Args:
        files: Archivos de origen, todos del mismo formato.
        schema: Esquema común, obtenido con `common_schema`.
        base_dir: Directorio base del dataset particionado, o `None`.

    Yields:
        pa.RecordBatch: Lotes con el esquema `schema`.
    """
    keys = {k for f in files for k in _partitions(f, base_dir)} if base_dir else set()
    data_schema = pa.schema([fld for fld in schema if fld.name not in keys])

    for f in files:
        parts = _partitions(f, base_dir) if base_dir else {}

        if input_format(f) == 'parquet':
            batches: Iterable[pa.RecordBatch] = pq.ParquetFile(f).iter_batches()

        else:
            batches = _open_csv(f, data_schema)

        for batch in batches:
            arrays = list(batch.columns)

            for fld in list(schema)[len(data_schema):]:
                value = parts.get(fld.name)
                arrays.append(
                    pa.repeat(pa.scalar(value).cast(fld.type), batch.num_rows)
                    if value is not None
                    else pa.nulls(batch.num_rows, fld.type)
                )

            yield pa.RecordBatch.from_arrays(arrays, schema=schema)


# =============================================================================
def remove_target(target: Path) -> None:
    """Elimina el destino si existe, ya sea un archivo o un directorio.

    Args:
        target: Ruta del archivo o directorio a eliminar.
    """
    if target.is_dir():
        shutil.rmtree(target)

    elif target.exists():
        target.unlink()


# =============================================================================
def write_file(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    target: Path,
    format_: str,
    compression: str
) -> None:
    """Escribe lotes de registros en un único archivo Parquet o CSV.

    Args:
        batches: Lotes de registros a escribir, en orden.
        schema: Esquema común a todos los lotes.
        target: Ruta del archivo de salida; se crean los directorios
            intermedios que falten.
        format_: Formato de destino ('parquet' o 'csv').
        compression: Códec de compresión ya resuelto para el formato.
    """
    target.parent.mkdir(parents=True, exist_ok=True)

    if format_ == 'parquet':
        with pq.ParquetWriter(target, schema, compression=compression) as w:
            for b in batches:
                w.write_batch(b)

    elif compression == 'none':
        with pacsv.CSVWriter(target, schema) as w:
            for b in batches:
                w.write_batch(b)
    else:
        with pa.CompressedOutputStream(str(target), compression) as out, \
                pacsv.CSVWriter(out, schema) as w:
            for b in batches:
                w.write_batch(b)


# =============================================================================
def write_partitioned(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    target: Path,
    format_: str,
    compression: str,
    partition: list[str]
) -> None:
    """Escribe lotes de registros como dataset particionado estilo Hive.

    Las columnas de partición se guardan en los nombres de los
    directorios (`columna=valor`) y no dentro de los archivos. Cada lote se
    reparte y se escribe en cuanto llega, con un archivo abierto por
    partición, por lo que el uso de memoria no crece con el volumen de
    datos.

    Args:
        batches: Lotes de registros a escribir.
        schema: Esquema común a todos los lotes.
        target: Directorio raíz del dataset de salida.
        format_: Formato de destino ('parquet' o 'csv').
        compression: Códec de compresión ya resuelto; en CSV debe ser
            'none'.
        partition: Columnas por las que se particiona.
    """
    data_schema = pa.schema([f for f in schema if f.name not in partition])
    writers: dict[tuple, pq.ParquetWriter | pacsv.CSVWriter] = {}

    try:
        for batch in batches:
            table = pa.Table.from_batches([batch], schema=schema)
            keys = table.select(partition).group_by(partition).aggregate([])

            for key in keys.to_pylist():
                mask = None

                for col, value in key.items():
                    cond = (
                        pc.is_null(table[col]) if value is None
                        else pc.equal(table[col], value)
                    )
                    mask = cond if mask is None else pc.and_(mask, cond)

                part = table.filter(mask).select(data_schema.names)
                values = tuple(key.values())

                if values not in writers:
                    writers[values] = _open_partition(
                        target, key, data_schema, format_, compression
                    )

                writers[values].write_table(part)

    finally:
        for writer in writers.values():
            writer.close()


# =============================================================================
def _open_partition(
    target: Path,
    key: dict[str, object],
    schema: pa.Schema,
    format_: str,
    compression: str
) -> pq.ParquetWriter | pacsv.CSVWriter:
    """Crea el directorio de una partición y abre su archivo de escritura.

    Args:
        target: Directorio raíz del dataset.
        key: Valor de cada columna de partición; `None` se escribe como
            `__HIVE_DEFAULT_PARTITION__`.
        schema: Esquema de los datos, sin las columnas de partición.
        format_: Formato de destino ('parquet' o 'csv').
        compression: Códec de compresión ya resuelto para el formato.

    Returns:
        pq.ParquetWriter | pacsv.CSVWriter: Escritor del archivo
            `part-0` de la partición.
    """
    folder = target.joinpath(*(
        f'{col}={HIVE_NULL if value is None else value}'
        for col, value in key.items()
    ))
    folder.mkdir(parents=True, exist_ok=True)

    if format_ == 'parquet':
        return pq.ParquetWriter(folder / 'part-0.parquet', schema, compression=compression)

    return pacsv.CSVWriter(folder / 'part-0.csv', schema)
