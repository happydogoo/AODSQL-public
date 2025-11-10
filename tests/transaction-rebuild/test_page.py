import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import pytest
from src.storage import HeapPage
from src.engine.transaction.transaction import Transaction, IsolationLevel

def make_transaction():
    return Transaction(1, IsolationLevel.SERIALIZABLE)

@pytest.fixture
def dummy_transaction():
    return make_transaction()

def test_heap_page_lsn_persistence():
    p1 = HeapPage(page_id=1)
    p1.set_page_lsn(999)
    data = p1.to_bytes()
    p2 = HeapPage(page_id=1, data=data)
    assert p2.get_page_lsn() == 999

@pytest.mark.parametrize('op,lsn', [
    ("insert", 100),
    ("update", 200),
    ("delete", 300),
])
def test_heap_page_modification_methods_set_lsn(dummy_transaction, op, lsn):
    page = HeapPage(page_id=1)
    # 先插入一条数据，保证key_count>0
    rid = page.insert_record(dummy_transaction, b'data', lsn=50)
    if op == "insert":
        page.insert_record(dummy_transaction, b'data2', lsn=lsn)
        assert page.get_page_lsn() == lsn
    elif op == "update":
        page.update_record(dummy_transaction, 0, b'new_data', lsn=lsn)
        assert page.get_page_lsn() == lsn
    elif op == "delete":
        page.mark_as_deleted(dummy_transaction, 0, len(b'data'), lsn=lsn)
        assert page.get_page_lsn() == lsn 