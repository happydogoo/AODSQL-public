import pytest
from src.engine.transaction.transaction_manager import TransactionManager
from src.engine.transaction.transaction import IsolationLevel, TransactionState
from src.engine.transaction.lock_manager import ResourceID, LockMode

class DummyTxn:
    def __init__(self, txn_id, isolation_level=IsolationLevel.REPEATABLE_READ):
        self.id = txn_id
        self.state = TransactionState.ACTIVE
        self.isolation_level = isolation_level
        self.last_lsn = 0
        self._held_locks = set()
    def set_state(self, s):
        self.state = s
    def get_held_locks(self):
        return self._held_locks
    def add_held_lock(self, rid):
        self._held_locks.add(rid)
    def remove_all_held_locks(self):
        self._held_locks.clear()

class DummyLockManager:
    def __init__(self):
        self.acquired = []
        self.released = []
    def acquire(self, txn, mode, rid):
        self.acquired.append((txn.id, mode, rid))
        txn.add_held_lock(rid)
    def release_all(self, txn):
        self.released.append(txn.id)
        txn.remove_all_held_locks()

class DummyLogManager:
    def __init__(self):
        self.logs = []
        self.flushed = []
    def append(self, txn, log):
        self.logs.append((txn.id, log))
        return len(self.logs)
    def flush_to_lsn(self, lsn):
        self.flushed.append(lsn)
    def read_log_record_by_lsn(self, lsn):
        return None

@pytest.fixture
def txn_mgr():
    return TransactionManager(DummyLockManager(), DummyLogManager())

def test_begin_commit_abort(txn_mgr):
    txn = txn_mgr.begin(IsolationLevel.READ_COMMITTED)
    assert txn.id == 1
    assert txn.state.name == 'ACTIVE'
    assert txn.isolation_level == IsolationLevel.READ_COMMITTED
    # commit
    assert txn_mgr.commit(txn)
    # 再次 commit 应异常
    txn2 = txn_mgr.begin()
    txn2.set_state(TransactionState.COMMITTED)
    with pytest.raises(Exception):
        txn_mgr.commit(txn2)
    # abort
    txn3 = txn_mgr.begin()
    assert txn_mgr.abort(txn3)
    # 已结束事务 abort
    txn3.set_state(TransactionState.ABORTED)
    assert txn_mgr.abort(txn3)

def test_acquire_release_lock(txn_mgr):
    txn = txn_mgr.begin()
    rid = ResourceID('t', 1, 1)
    txn_mgr.acquire_lock(txn, LockMode.SHARED, rid)
    assert rid in txn.get_held_locks()
    txn_mgr.release_lock(txn, rid)  # S2PL下不实际释放

def test_log_update_and_flush(txn_mgr):
    txn = txn_mgr.begin()
    rid = ResourceID('t', 1, 1)
    before = b'b'
    after = b'a'
    lsn = txn_mgr.log_update(txn, rid, before, after)
    assert lsn > 0
    txn.last_lsn = lsn
    txn_mgr.force_log_flush(txn)

def test_transaction_context_manager(txn_mgr):
    with txn_mgr.transaction(IsolationLevel.SERIALIZABLE) as txn:
        assert txn.isolation_level == IsolationLevel.SERIALIZABLE
        assert txn.state == TransactionState.ACTIVE
    # 事务应已提交
    assert txn.state == TransactionState.COMMITTED
    # 异常自动回滚
    try:
        with txn_mgr.transaction() as txn2:
            raise ValueError('fail')
    except ValueError:
        assert txn2.state == TransactionState.ABORTED 