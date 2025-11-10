import pytest
from src.engine.operator import Insert, Update, CreateTable, Delete, ShowTables, Operator, Schema
from src.engine.transaction.transaction import Transaction
from unittest.mock import MagicMock

class DummyStorage:
    def __init__(self):
        self.inserted = []
        self.updated = []
        self.deleted = []
        self.created = []
    def insert_row(self, txn, table, row):
        self.inserted.append((txn, table, row))
    def update_row(self, txn, table, row_id, new_row):
        self.updated.append((txn, table, row_id, new_row))
    def delete_row(self, txn, table, row_id):
        self.deleted.append((txn, table, row_id))
    def create_table(self, txn, table, columns):
        self.created.append((txn, table, columns))
    def scan(self, txn, table):
        return [(1, (1, 'a'))]

def test_insert_execute():
    storage = DummyStorage()
    op = Insert('t', [(1, 'a')], storage)
    op.transaction = Transaction(1, "READ_COMMITTED")
    result = op.execute()
    assert 'inserted' in result
    assert storage.inserted

def test_update_execute():
    storage = DummyStorage()
    child = MagicMock()
    child.next.side_effect = [[(1, (1, 'a'))], None]
    op = Update(child, 't', {1: 'b'}, storage)
    op.transaction = Transaction(2, "READ_COMMITTED")
    result = op.execute()
    assert 'updated' in result
    assert storage.updated

def test_create_table_execute():
    storage = DummyStorage()
    op = CreateTable('t', [('id', 'INT')], storage)
    op.transaction = Transaction(3, "READ_COMMITTED")
    result = op.execute()
    assert 'created' in result or 'created' in result.lower() or 'created' in str(result)
    assert storage.created

def test_delete_execute():
    storage = DummyStorage()
    child = MagicMock()
    # row_id 改为 (1, 1)
    child.next.side_effect = [[((1, 1), (1, 'a'))], None]
    op = Delete(child, 't', storage)
    op.transaction = Transaction(4, "READ_COMMITTED")
    result = op.execute()
    assert 'deleted' in result
    assert storage.deleted

def test_show_tables_next():
    catalog = MagicMock()
    catalog.list_tables.return_value = ['t1', 't2']
    op = ShowTables(catalog)
    batch = op.next()
    assert batch and batch[0][1][0] == 't1' 