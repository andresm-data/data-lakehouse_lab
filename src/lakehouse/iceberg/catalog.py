"""Configura el catálogo SQL (SQLite) de Iceberg y crea el namespace."""
import argparse
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

from lakehouse.paths import DATA_DIR


# =============================================================================
CATALOG_NAME = 'lakehouse'
CATALOG_DB = DATA_DIR / 'iceberg/catalog.db'
WAREHOUSE = DATA_DIR / 'iceberg/warehouse'
NAMESPACE = 'lakehouse'


# =============================================================================
def get_catalog(db: Path = CATALOG_DB, warehouse: Path = WAREHOUSE) -> Catalog:
    """Abre (o crea) el catálogo SQL de Iceberg respaldado por SQLite.

    El archivo `.db` guarda solo el puntero de cada tabla a su archivo de
    metadatos actual; los datos y metadatos de Iceberg van en `warehouse`.

    Args:
        db (Path): Archivo SQLite del catálogo; se crea si no existe.
        warehouse (Path): Directorio donde Iceberg escribe las tablas.

    Returns:
        Catalog: Catálogo listo para usar.
    """
    db.parent.mkdir(parents=True, exist_ok=True)
    warehouse.mkdir(parents=True, exist_ok=True)

    return load_catalog(
        CATALOG_NAME,
        type='sql',
        uri=f'sqlite:///{db.resolve()}',
        warehouse=warehouse.resolve().as_uri(),
    )


# =============================================================================
def ensure_namespace(catalog: Catalog, namespace: str = NAMESPACE) -> None:
    """Crea el namespace si todavía no existe."""
    catalog.create_namespace_if_not_exists(namespace)


# =============================================================================
def main() -> int:
    """Crea el catálogo y el namespace, y lista su contenido.

    Returns:
        int: `0` siempre.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalogo', type=Path, default=CATALOG_DB)
    parser.add_argument('--warehouse', type=Path, default=WAREHOUSE)
    parser.add_argument('--namespace', default=NAMESPACE)
    args = parser.parse_args()

    catalog = get_catalog(args.catalogo, args.warehouse)
    ensure_namespace(catalog, args.namespace)

    print(f'Catálogo:  {args.catalogo}')
    print(f'Warehouse: {args.warehouse}')
    print('Namespaces:')

    for ns in catalog.list_namespaces():
        tables = catalog.list_tables(ns)
        print(f'  {".".join(ns)} ({len(tables)} tablas)')

        for t in tables:
            print(f'    - {".".join(t)}')

    return 0


# =============================================================================
if __name__ == "__main__":
    raise SystemExit(main())
