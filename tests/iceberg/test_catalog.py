"""Pruebas unitarias de lakehouse.iceberg.catalog."""
import sys

from lakehouse.iceberg.catalog import ensure_namespace, get_catalog, main


# =============================================================================
class TestGetCatalog:

    def test_crea_archivo_y_warehouse(self, tmp_path):
        db = tmp_path / 'cat' / 'catalog.db'
        warehouse = tmp_path / 'wh'

        catalog = get_catalog(db, warehouse)
        ensure_namespace(catalog)

        assert db.exists()
        assert warehouse.is_dir()
        assert catalog.properties['warehouse'] == warehouse.resolve().as_uri()

    def test_namespace_es_idempotente(self, tmp_path):
        catalog = get_catalog(tmp_path / 'c.db', tmp_path / 'wh')

        ensure_namespace(catalog, 'demo')
        ensure_namespace(catalog, 'demo')

        assert catalog.list_namespaces() == [('demo',)]

    def test_persiste_entre_aperturas(self, tmp_path):
        ensure_namespace(get_catalog(tmp_path / 'c.db', tmp_path / 'wh'), 'demo')

        catalog = get_catalog(tmp_path / 'c.db', tmp_path / 'wh')

        assert ('demo',) in catalog.list_namespaces()


# =============================================================================
class TestMain:

    def test_lista_namespaces(self, monkeypatch, capsys, tmp_path):
        monkeypatch.setattr(sys, 'argv', [
            'lh-iceberg-catalog', '--catalogo', str(tmp_path / 'c.db'),
            '--warehouse', str(tmp_path / 'wh'), '--namespace', 'demo',
        ])

        assert main() == 0
        assert 'demo (0 tablas)' in capsys.readouterr().out
