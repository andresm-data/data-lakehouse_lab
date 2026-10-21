"""Operaciones sobre tablas Iceberg: evolución de partición y rollback."""
from pyiceberg.table import Table
from pyiceberg.table.snapshots import ancestors_of
from pyiceberg.transforms import Transform

from lakehouse.iceberg.create_table import describe_spec
from lakehouse.iceberg.snapshots import snapshot_rows


# =============================================================================
def evolve_partition_spec(table: Table, source_column: str,
                          transform: Transform, name: str | None = None,
                          remove: str | None = None) -> Table:
    """Cambia la partición de la tabla sin reescribir sus datos.

    Crea una spec nueva y la deja como actual. Los archivos ya escritos
    siguen con su `spec_id` anterior; solo las escrituras nuevas usan la
    spec nueva.

    Args:
        table (Table): Tabla Iceberg.
        source_column (str): Columna sobre la que se aplica la transformación.
        transform (Transform): Transformación, por ejemplo `DayTransform()`.
        name (str | None): Nombre del campo de partición; si es `None`,
            Iceberg lo genera (por ejemplo `tpep_pickup_datetime_day`).
        remove (str | None): Campo de partición a quitar en el mismo cambio,
            por ejemplo `pickup_month` para pasar de mes a día.

    Returns:
        Table: La misma tabla, con la spec nueva.
    """
    with table.update_spec() as spec:
        if remove is not None:
            spec.remove_field(remove)

        spec.add_field(source_column, transform, name)

    return table


# =============================================================================
def list_specs(table: Table) -> list[dict]:
    """Specs de partición de la tabla, de la más antigua a la actual.

    Returns:
        list[dict]: `spec_id`, `descripcion` y `actual` (si es la spec
            que usan las escrituras nuevas).
    """
    current = table.spec().spec_id

    return [
        {'spec_id': spec_id, 'descripcion': describe_spec(table, spec),
         'actual': spec_id == current}
        for spec_id, spec in sorted(table.specs().items())
    ]


# =============================================================================
def files_by_spec(table: Table) -> list[dict]:
    """Archivos de datos y filas del snapshot actual por `spec_id`.

    Returns:
        list[dict]: `spec_id`, `archivos` y `filas`, ordenadas por `spec_id`.
            Si la tabla no tiene snapshots, la lista está vacía.
    """
    totals: dict[int, dict] = {}

    for row in table.inspect.files().select(['spec_id', 'record_count']).to_pylist():
        entry = totals.setdefault(
            row['spec_id'], {'spec_id': row['spec_id'], 'archivos': 0, 'filas': 0}
        )
        entry['archivos'] += 1
        entry['filas'] += row['record_count']

    return [totals[k] for k in sorted(totals)]


# =============================================================================
def list_snapshots(table: Table) -> list[dict]:
    """Historial de snapshots, del más antiguo al actual.

    Agrega a cada fila de `snapshot_rows` la clave `en_linea_actual`: si el
    snapshot es ancestro del actual (o el actual). Después de un rollback,
    los snapshots descartados siguen en la lista con `en_linea_actual` en
    `False`, y se pueden leer con `table.scan(snapshot_id=...)`.

    Returns:
        list[dict]: Una fila por snapshot.
    """
    lineage = {
        s.snapshot_id for s in ancestors_of(table.current_snapshot(), table.metadata)
    }

    return [
        {**row, 'en_linea_actual': row['snapshot_id'] in lineage}
        for row in snapshot_rows(table)
    ]


# =============================================================================
def rollback_to(table: Table, snapshot_id: int) -> Table:
    """Vuelve la tabla a un snapshot anterior moviendo el puntero `main`.

    No reescribe datos ni crea un snapshot nuevo: el snapshot destino pasa a
    ser el actual y los posteriores quedan en el historial.

    Args:
        table (Table): Tabla Iceberg.
        snapshot_id (int): Snapshot destino; debe ser ancestro del actual.

    Returns:
        Table: La misma tabla, ya en el snapshot destino.

    Raises:
        ValueError: Si el snapshot no existe o no es ancestro del actual. La
            tabla no cambia.
    """
    try:
        table.manage_snapshots().rollback_to_snapshot(snapshot_id).commit()

    except ValueError as e:
        raise ValueError(f'No se puede volver al snapshot {snapshot_id}: {e}') from e

    return table
