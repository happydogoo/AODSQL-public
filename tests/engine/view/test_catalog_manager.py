import pytest
from src.engine.catalog_manager import CatalogManager, ColumnInfo, TableInfo, IndexInfo, ViewInfo, TriggerInfo
from src.engine.transaction.transaction import Transaction
import os
import tempfile

def make_catalog():
    # 使用临时文件，避免污染
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp.close()
    return CatalogManager(catalog_path=tmp.name), tmp.name

def test_create_and_get_table():
    catalog, path = make_catalog()
    txn = Transaction(1, "READ_COMMITTED")
    catalog.create_table(txn, 't1', [('id', 'INT'), ('name', 'VARCHAR')])
    assert catalog.table_exists('t1')
    tinfo = catalog.get_table('t1')
    assert tinfo.table_name == 't1'
    os.remove(path)

def test_create_duplicate_table():
    catalog, path = make_catalog()
    txn = Transaction(2, "READ_COMMITTED")
    catalog.create_table(txn, 't2', [('id', 'INT')])
    with pytest.raises(Exception):
        catalog.create_table(txn, 't2', [('id', 'INT')])
    os.remove(path)

def test_create_and_delete_index():
    catalog, path = make_catalog()
    txn = Transaction(3, "READ_COMMITTED")
    catalog.create_table(txn, 't3', [('id', 'INT')])
    ok, msg = catalog.create_index(txn, 't3', 'idx1', ['id'], 'f.idx', [1])
    assert ok
    assert catalog.has_index_on('t3', 'id')
    os.remove(path)

def test_create_and_get_view():
    catalog, path = make_catalog()
    catalog.create_view('v1', 'SELECT * FROM t1')
    assert catalog.view_exists('v1')
    vinfo = catalog.get_view('v1')
    assert vinfo.view_name == 'v1'
    catalog.delete_view('v1')
    assert not catalog.view_exists('v1')
    os.remove(path)

def test_create_and_get_trigger():
    catalog, path = make_catalog()
    catalog.create_table(Transaction(4, "READ_COMMITTED"), 't4', [('id', 'INT')])
    ok, msg = catalog.create_trigger('trg1', 't4', 'BEFORE', ['INSERT'], True, None, ['SELECT 1'])
    assert ok
    assert catalog.trigger_exists('trg1')
    tinfo = catalog.get_trigger('trg1')
    assert tinfo.trigger_name == 'trg1'
    ok, msg = catalog.delete_trigger('trg1')
    assert ok
    os.remove(path) 