import pytest
from src.engine.transaction.lock_manager import LockManager, LockMode, ResourceID, DeadlockError

class DummyTxn:
    def __init__(self, txn_id):
        self.id = txn_id
        self.held_locks = set()
    def add_held_lock(self, rid):
        self.held_locks.add(rid)
    def get_held_locks(self):
        return self.held_locks
    def remove_all_held_locks(self):
        self.held_locks.clear()

@pytest.fixture
def lock_mgr():
    return LockManager()

def test_acquire_and_release_shared(lock_mgr):
    txn1 = DummyTxn(1)
    rid = ResourceID('t1', 1, 1)
    lock_mgr.acquire(txn1, LockMode.SHARED, rid)
    assert rid in txn1.held_locks
    lock_mgr.release_all(txn1)
    assert len(txn1.held_locks) == 0

def test_shared_compatible(lock_mgr):
    txn1 = DummyTxn(1)
    txn2 = DummyTxn(2)
    rid = ResourceID('t1', 1, 1)
    lock_mgr.acquire(txn1, LockMode.SHARED, rid)
    lock_mgr.acquire(txn2, LockMode.SHARED, rid)
    assert rid in txn1.held_locks and rid in txn2.held_locks
    lock_mgr.release_all(txn1)
    lock_mgr.release_all(txn2)

def test_intention_lock(lock_mgr):
    txn1 = DummyTxn(1)
    table_rid = ResourceID('t1')
    page_rid = ResourceID('t1', 1)
    record_rid = ResourceID('t1', 1, 1)
    lock_mgr.acquire(txn1, LockMode.INTENTION_EXCLUSIVE, table_rid)
    lock_mgr.acquire(txn1, LockMode.INTENTION_EXCLUSIVE, page_rid)
    lock_mgr.acquire(txn1, LockMode.EXCLUSIVE, record_rid)
    assert record_rid in txn1.held_locks
    lock_mgr.release_all(txn1)

def test_deadlock_detection(lock_mgr):
    txn1 = DummyTxn(1)
    txn2 = DummyTxn(2)
    rid1 = ResourceID('t1', 1, 1)
    rid2 = ResourceID('t1', 1, 2)
    lock_mgr.acquire(txn1, LockMode.EXCLUSIVE, rid1)
    lock_mgr.acquire(txn2, LockMode.EXCLUSIVE, rid2)
    # 模拟死锁：txn1 等待 rid2，txn2 等待 rid1
    lock_mgr._lock_table[rid2].granted_locks[txn1.id] = LockMode.EXCLUSIVE
    lock_mgr._waits_for_graph[txn2.id] = {txn1.id}
    lock_mgr._waits_for_graph[txn1.id] = {txn2.id}
    assert lock_mgr._detect_deadlock(txn1.id, set()) 