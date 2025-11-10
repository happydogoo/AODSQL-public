import os
import tempfile
import pytest
from src.engine.transaction.log_manager import LogManager, LogType, InsertLogRecord, DeleteLogRecord, UpdateLogRecord, CommitLogRecord, AbortLogRecord, NULL_LSN
from src.engine.transaction.lock_manager import ResourceID

class DummyTxn:
    def __init__(self, txn_id):
        self.id = txn_id
        self.last_lsn = 0
    def __hash__(self):
        return hash(self.id)
    def __eq__(self, other):
        return isinstance(other, DummyTxn) and self.id == other.id

class DummyStorage:
    def __init__(self):
        self.catalog_manager = type('cat', (), {})()
        self.buffer_pools = {}
    def get_page_for_recovery(self, resource_id):
        class DummyPage:
            def get_page_lsn(self): return 0
            def mark_as_deleted(self, *a, **k): pass
            def insert_record(self, *a, **k): pass
            def update_record(self, *a, **k): pass
        return DummyPage()

@pytest.fixture
def log_mgr():
    tmpfile = tempfile.NamedTemporaryFile(delete=False)
    tmpfile.close()
    mgr = LogManager(tmpfile.name, DummyStorage())
    yield mgr
    mgr._log_file.close()
    os.remove(tmpfile.name)

def test_append_and_read(log_mgr):
    txn = DummyTxn(1)
    rid = ResourceID('t1', 1, 1)
    data = b'abc'
    log = InsertLogRecord(txn.id, rid, data)
    lsn = log_mgr.append(txn, log)
    log_mgr.flush_to_lsn(lsn)
    rec = log_mgr.read_log_record_by_lsn(lsn)
    assert isinstance(rec, InsertLogRecord)
    assert rec.resource_id == rid
    assert rec.inserted_data == data

def test_commit_abort_log(log_mgr):
    txn = DummyTxn(2)
    commit = CommitLogRecord(txn.id)
    abort = AbortLogRecord(txn.id)
    lsn1 = log_mgr.append(txn, commit)
    lsn2 = log_mgr.append(txn, abort)
    log_mgr.flush_to_lsn(lsn2)
    rec1 = log_mgr.read_log_record_by_lsn(lsn1)
    rec2 = log_mgr.read_log_record_by_lsn(lsn2)
    assert isinstance(rec1, CommitLogRecord)
    assert isinstance(rec2, AbortLogRecord)

def test_update_delete_log(log_mgr):
    txn = DummyTxn(3)
    rid = ResourceID('t2', 2, 2)
    before = b'before'
    after = b'after'
    update = UpdateLogRecord(txn.id, rid, before, after)
    lsn = log_mgr.append(txn, update)
    log_mgr.flush_to_lsn(lsn)
    rec = log_mgr.read_log_record_by_lsn(lsn)
    assert isinstance(rec, UpdateLogRecord)
    assert rec.before_image == before
    assert rec.after_image == after
    delete = DeleteLogRecord(txn.id, rid, before)
    lsn2 = log_mgr.append(txn, delete)
    log_mgr.flush_to_lsn(lsn2)
    rec2 = log_mgr.read_log_record_by_lsn(lsn2)
    assert isinstance(rec2, DeleteLogRecord)
    assert rec2.deleted_data == before

def test_logmanager_recover(log_mgr):
    # 只测试 recover 能正常执行，不抛异常
    log_mgr.recover() 