import os
import pytest
from src.engine.catalog_manager import CatalogManager
from src.engine.storage.real_storage_engine import RealStorageEngine
from src.engine.transaction.log_manager import LogManager
from src.engine.transaction.lock_manager import LockManager
from src.engine.transaction.transaction import Transaction, IsolationLevel
from src.engine.transaction.transaction_manager import TransactionManager

def user_schema():
    return [('name', 'STR(20)'), ('age', 'int')]

@pytest.fixture
def setup_env(tmp_path):
    data_dir = tmp_path / 'db'
    os.makedirs(data_dir, exist_ok=True)
    catalog = CatalogManager()
    lock_manager = LockManager()
    log_manager = LogManager(str(tmp_path / 'test.log'), None)
    rse = RealStorageEngine(catalog, log_manager, lock_manager, data_dir=str(data_dir))
    log_manager._storage_engine = rse
    return rse, log_manager, lock_manager, catalog

def test_insert_redo(setup_env):
    rse, log_manager, lock_manager, catalog = setup_env
    tm = TransactionManager(lock_manager, log_manager)
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', user_schema())
    tm.commit(txn1)
    txn2 = tm.begin()
    row_id = rse.insert_row(txn2, 'users', ('bob', 30))
    tm.commit(txn2)
    # 读取插入前的日志内容
    log_records = []
    with open(log_manager._log_file_path, 'rb') as f:
        while True:
            len_prefix_bytes = f.read(4)
            if not len_prefix_bytes or len(len_prefix_bytes) < 4:
                break
            record_len = int.from_bytes(len_prefix_bytes, 'little')
            record_bytes = f.read(record_len)
            if not record_bytes or len(record_bytes) < 8:
                break
            from src.engine.transaction.log_manager import LogRecord
            record = LogRecord.from_bytes(record_bytes)
            if record is not None:
                log_records.append(record)
    # 模拟崩溃，重建环境
    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=rse.data_dir)
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    # 检查重做后数据是否存在
    txn3 = Transaction(999, IsolationLevel.REPEATABLE_READ)
    results = list(rse2.scan(txn3, 'users'))
    assert any(row[1] == ('bob', 30) for row in results) 