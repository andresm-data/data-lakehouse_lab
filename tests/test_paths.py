"""Pruebas de los directorios base configurables por variables de entorno."""
import importlib
from pathlib import Path

import lakehouse.paths as paths


def _reload(monkeypatch, **env):
    for name in ('LAKEHOUSE_DATA_DIR', 'LAKEHOUSE_DOCS_DIR'):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return importlib.reload(paths)


def test_por_defecto_son_relativos(monkeypatch):
    mod = _reload(monkeypatch)
    assert mod.DATA_DIR == Path('data')
    assert mod.DOCS_DIR == Path('docs')
    # Mismo resultado que las rutas literales de antes.
    assert mod.DATA_DIR / 'iceberg/catalog.db' == Path('data/iceberg/catalog.db')


def test_se_pueden_cambiar_con_variables(monkeypatch, tmp_path):
    mod = _reload(monkeypatch, LAKEHOUSE_DATA_DIR=str(tmp_path / 'd'),
                  LAKEHOUSE_DOCS_DIR=str(tmp_path / 'r'))
    assert mod.DATA_DIR == tmp_path / 'd'
    assert mod.DOCS_DIR == tmp_path / 'r'
    _reload(monkeypatch)
