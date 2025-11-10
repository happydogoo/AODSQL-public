import pytest
from src.engine.storage.heap_file_manager import HeapFileManager

class DummyPage:
    _id_counter = 0
    def __init__(self):
        self.records = {}
        self.key_count = 0
        self.next_page_id = None
        self.page_id = DummyPage._id_counter
        DummyPage._id_counter += 1
    def insert_record(self, txn, record_bytes, lsn):
        rid = self.key_count
        self.records[rid] = record_bytes
        self.key_count += 1
        return rid
    def update_record(self, txn, rid, record_bytes, lsn):
        self.records[rid] = record_bytes
    def mark_as_deleted(self, txn, rid, size, lsn):
        self.records.pop(rid, None)
    def get_record(self, rid, size):
        if rid in self.records:
            return True, self.records[rid]
        return False, b''
    def is_full(self, size):
        return False

class DummyBufferPool:
    def __init__(self):
        self.page = DummyPage()
    def new_page(self, page_cls=None):
        return self.page
    def get_page(self, page_id):
        return self.page
    def unpin_page(self, page_id, is_dirty):
        pass

def test_insert_and_get():
    table_info = type('tbl', (), {'last_page_id': None, 'root_page_id': None})()
    hfm = HeapFileManager(DummyBufferPool(), table_info, None)
    txn = object()
    data = b'abc'
    row_id = hfm.insert_record(txn, data, 1)
    # 兼容 DummyPage 的 page_id 生成逻辑
    assert row_id[1] == 0
    got = hfm.get_record(txn, row_id, len(data))
    assert got == data

def test_update():
    table_info = type('tbl', (), {'last_page_id': None, 'root_page_id': None})()
    hfm = HeapFileManager(DummyBufferPool(), table_info, None)
    txn = object()
    data = b'abc'
    row_id = hfm.insert_record(txn, data, 1)
    hfm.update_record(txn, row_id, b'def', 2)
    got = hfm.get_record(txn, row_id, 3)
    assert got == b'def'

def test_delete():
    table_info = type('tbl', (), {'last_page_id': None, 'root_page_id': None})()
    hfm = HeapFileManager(DummyBufferPool(), table_info, None)
    txn = object()
    data = b'abc'
    row_id = hfm.insert_record(txn, data, 1)
    hfm.delete_record(txn, row_id, len(data), 2)
    with pytest.raises(Exception):
        hfm.get_record(txn, row_id, len(data))

def test_scan():
    table_info = type('tbl', (), {'last_page_id': None, 'root_page_id': None})()
    hfm = HeapFileManager(DummyBufferPool(), table_info, None)
    txn = object()
    data1 = b'abc'
    data2 = b'def'
    hfm.insert_record(txn, data1, 1)
    hfm.insert_record(txn, data2, 2)
    results = list(hfm.scan(txn, 3))
    assert len(results) == 2
    assert results[0][1] == data1
    assert results[1][1] == data2 