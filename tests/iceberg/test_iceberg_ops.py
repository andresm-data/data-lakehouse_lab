"""Pruebas de lakehouse.iceberg.ops: evolución de partición y rollback."""
import pyarrow.compute as pc
import pytest
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.transforms import DayTransform, IdentityTransform

from lakehouse.iceberg.create_table import (
    DATE_COLUMN, IDENTIFIER, PARTITION_NAME, create_table,
)
from lakehouse.iceberg.ops import (
    evolve_partition_spec, files_by_spec, list_snapshots, list_specs,
    rollback_to,
)


# =============================================================================
def _month(data, month: int):
    """Filas de `data` de un mes de 2025."""
    return data.filter(pc.equal(data['month'], month))


@pytest.fixture
def data(make_table, rows):
    """Datos de enero a marzo de 2025."""
    return make_table(rows)


@pytest.fixture
def table(catalog, data):
    """Tabla particionada por mes con enero y febrero cargados."""
    t = create_table(catalog, IDENTIFIER, data.schema)
    t.append(data.filter(pc.less(data['month'], 3)))
    return t


@pytest.fixture
def evolved(table, data):
    """La tabla anterior pasada a `day()` y con marzo escrito después."""
    evolve_partition_spec(
        table, DATE_COLUMN, DayTransform(), 'pickup_day', remove=PARTITION_NAME,
    )
    table.append(_month(data, 3))
    return table


# =============================================================================
class TestEvolvePartitionSpec:

    def test_dos_specs_y_la_nueva_es_la_actual(self, table):
        evolve_partition_spec(
            table, DATE_COLUMN, DayTransform(), 'pickup_day', remove=PARTITION_NAME,
        )

        assert list_specs(table) == [
            {'spec_id': 0, 'actual': False,
             'descripcion': 'month(tpep_pickup_datetime) -> pickup_month'},
            {'spec_id': 1, 'actual': True,
             'descripcion': 'day(tpep_pickup_datetime) -> pickup_day'},
        ]

    def test_no_reescribe_archivos_viejos(self, table):
        before = table.inspect.files()['file_path'].to_pylist()

        evolve_partition_spec(
            table, DATE_COLUMN, DayTransform(), 'pickup_day', remove=PARTITION_NAME,
        )

        assert table.inspect.files()['file_path'].to_pylist() == before

    def test_archivos_viejos_y_nuevos_con_spec_distinta(self, evolved):
        assert files_by_spec(evolved) == [
            {'spec_id': 0, 'archivos': 2, 'filas': 4},
            {'spec_id': 1, 'archivos': 3, 'filas': 3},
        ]

    def test_conteo_total_es_la_suma(self, evolved, rows):
        assert evolved.scan().to_arrow().num_rows == len(rows)
        assert sum(f['filas'] for f in files_by_spec(evolved)) == len(rows)

    def test_particiones_por_spec(self, evolved):
        partitions = evolved.inspect.partitions().to_pylist()
        by_spec = {}

        for p in partitions:
            by_spec.setdefault(p['spec_id'], []).append(p['partition'])

        assert len(by_spec[0]) == 2
        assert all(p['pickup_day'] is None for p in by_spec[0])
        assert len(by_spec[1]) == 3
        assert all(p['pickup_month'] is None for p in by_spec[1])

    def test_consulta_filtrada_cruza_ambas_specs(self, evolved):
        row_filter = And(
            GreaterThanOrEqual(DATE_COLUMN, '2025-02-15T00:00:00'),
            LessThan(DATE_COLUMN, '2025-03-10T00:00:00'),
        )
        scan = evolved.scan(row_filter=row_filter)

        assert sorted(scan.to_arrow()['id'].to_pylist()) == [4, 5]
        # Febrero (spec 0) + solo el día 1 de marzo (spec 1).
        assert len(list(scan.plan_files())) == 2

    def test_nombre_automatico_y_sin_quitar_campos(self, table):
        evolve_partition_spec(table, 'id', IdentityTransform())

        assert table.spec().spec_id == 1
        assert [f.name for f in table.spec().fields] == [PARTITION_NAME, 'id']

    def test_tabla_sin_snapshots_no_tiene_archivos(self, catalog, data):
        empty = create_table(catalog, IDENTIFIER, data.schema)

        assert files_by_spec(empty) == []


# =============================================================================
@pytest.fixture
def bad_write(evolved, data):
    """Guarda el snapshot actual y repite el append de marzo por error.

    Returns:
        tuple[int, int]: (`snapshot_id` bueno, `snapshot_id` malo).
    """
    good = evolved.current_snapshot().snapshot_id
    evolved.append(_month(data, 3))
    return good, evolved.current_snapshot().snapshot_id


class TestRollback:

    def test_vuelve_al_snapshot_guardado(self, evolved, bad_write, rows):
        good, _ = bad_write
        assert evolved.scan().to_arrow().num_rows == len(rows) + 3

        rollback_to(evolved, good)

        assert evolved.current_snapshot().snapshot_id == good
        assert evolved.scan().to_arrow().num_rows == len(rows)

    def test_persiste_en_el_catalogo(self, catalog, evolved, bad_write):
        good, _ = bad_write

        rollback_to(evolved, good)

        reloaded = catalog.load_table(IDENTIFIER)
        assert reloaded.current_snapshot().snapshot_id == good

    def test_no_crea_snapshot_nuevo(self, evolved, bad_write):
        good, _ = bad_write
        before = len(evolved.snapshots())

        rollback_to(evolved, good)

        assert len(evolved.snapshots()) == before

    def test_snapshot_malo_sigue_en_el_historial(self, evolved, bad_write, rows):
        good, bad = bad_write

        rollback_to(evolved, good)

        history = {s['snapshot_id']: s for s in list_snapshots(evolved)}
        assert history[bad]['en_linea_actual'] is False
        assert history[bad]['actual'] is False
        assert history[good]['actual'] is True
        # Time travel al snapshot descartado.
        assert evolved.scan(snapshot_id=bad).to_arrow().num_rows == len(rows) + 3

    def test_historial_registra_el_cambio_de_puntero(self, evolved, bad_write):
        good, bad = bad_write

        rollback_to(evolved, good)

        assert [h.snapshot_id for h in evolved.history()][-2:] == [bad, good]


# =============================================================================
class TestRollbackErrors:

    def test_snapshot_inexistente(self, evolved, bad_write):
        _, bad = bad_write

        with pytest.raises(ValueError, match='No se puede volver al snapshot 123'):
            rollback_to(evolved, 123)

        assert evolved.current_snapshot().snapshot_id == bad

    def test_snapshot_que_no_es_ancestro(self, evolved, bad_write):
        good, bad = bad_write
        rollback_to(evolved, good)

        with pytest.raises(ValueError, match='not an ancestor'):
            rollback_to(evolved, bad)

        assert evolved.current_snapshot().snapshot_id == good


# =============================================================================
class TestListSnapshots:

    def test_todos_en_linea_sin_rollback(self, evolved):
        history = list_snapshots(evolved)

        assert len(history) == 2
        assert all(s['en_linea_actual'] for s in history)
        assert [s['actual'] for s in history] == [False, True]

    def test_tabla_sin_snapshots(self, catalog, data):
        empty = create_table(catalog, IDENTIFIER, data.schema)

        assert list_snapshots(empty) == []
