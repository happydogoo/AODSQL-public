import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import threading
import time
import pytest
from src.engine.catalog_manager import CatalogManager
from src.engine.storage.real_storage_engine import RealStorageEngine
from src.engine.transaction.log_manager import LogManager
from src.engine.transaction.lock_manager import LockManager
from src.engine.transaction.transaction_manager import TransactionManager
from loguru import logger
@pytest.fixture
def tm(setup_env):
    rse, log_manager, lock_manager, catalog = setup_env
    return TransactionManager(lock_manager, log_manager)

@pytest.fixture
def setup_env(tmp_path):
    data_dir = tmp_path / 'db'
    os.makedirs(data_dir, exist_ok=True)
    catalog = CatalogManager()
    lock_manager = LockManager()
    # 先用None占位，后面注入rse
    log_manager = LogManager(str(tmp_path / 'test.log'), None)
    rse = RealStorageEngine(catalog, log_manager, lock_manager, data_dir=str(data_dir))
    log_manager._storage_engine = rse
    return rse, log_manager, lock_manager, catalog

def user_schema():
    return [('name', 'STR(20)'), ('age', 'int')]

# 用例1：基本提交与读取
def test_simple_commit_and_read(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', user_schema())
    tm.commit(txn1)
    txn2 = tm.begin()
    rse.insert_row(txn2, 'users', ('alice', 25))
    tm.commit(txn2)
    txn3 = tm.begin()
    results = list(rse.scan(txn3, 'users'))
    tm.commit(txn3)
    assert len(results) == 1
    assert results[0][1] == ('alice', 25)

# 用例2：回滚
def test_simple_abort(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', user_schema())
    tm.commit(txn1)
    txn2 = tm.begin()
    rse.insert_row(txn2, 'users', ('bob', 30))
    tm.abort(txn2)
    txn3 = tm.begin()
    results = list(rse.scan(txn3, 'users'))
    tm.commit(txn3)
    assert len(results) == 0

# 用例3：隔离性
@pytest.mark.timeout(10)
def test_read_uncommitted_is_blocked(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', user_schema())
    rse.insert_row(txn1, 'users', ('alice', 25))
    tm.commit(txn1)
    # 插入后获取row_id
    txn2 = tm.begin()
    row_id = rse.insert_row(txn2, 'users', ('bob', 30))
    tm.commit(txn2)
    txn3 = tm.begin()
    results = list(rse.scan(txn3, 'users'))
    tm.commit(txn3)
    # 取alice的row_id
    alice_row_id = None
    for rid, row in results:
        if row[0] == 'alice':
            alice_row_id = rid
    assert alice_row_id is not None
    # 线程1：更新alice
    def t1():
        t1_txn = tm.begin()
        rse.update_row(t1_txn, 'users', alice_row_id, ('alice', 26))
        time.sleep(2)
        tm.commit(t1_txn)
    # 线程2：尝试scan
    scan_result = {}
    def t2():
        time.sleep(1)
        t2_txn = tm.begin()
        scan_result['res'] = list(rse.scan(t2_txn, 'users'))
        tm.commit(t2_txn)
    thread1 = threading.Thread(target=t1)
    thread2 = threading.Thread(target=t2)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    # 线程2应该读到更新后的数据
    found = False
    for rid, row in scan_result['res']:
        if row[0] == 'alice' and row[1] == 26:
            found = True
    assert found

# 用例4：崩溃恢复-已提交数据
def test_crash_recovery_for_committed_data(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', [('name', 'STR(20)'), ('age', 'int')])
    print("【test_crash_recovery_for_committed_data】", catalog.get_table_info('users'))
    tm.commit(txn1)
    txn2 = tm.begin()
    rse.insert_row(txn2, 'users', ('eve', 99))
    tm.commit(txn2)
    # 不调用flush/close，直接模拟崩溃
    # 重启

    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    # 修正日志文件路径，直接用log_manager._log_file_path
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=str(rse.data_dir))
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    txn3 = tm.begin()
    print("【test_crash_recovery_for_committed_data】", catalog2.get_table_info('users'))
    results = []
    for row in rse2.scan(txn3, 'users'):
        results.append(row)
    tm.commit(txn3)
    print("【test_crash_recovery_for_committed_data】", results)

    assert any(row[1] == ('eve', 99) for row in results)

# 用例5：崩溃恢复-未提交数据
# 让用例5也用 setup_env, tm fixture，保证环境一致

def test_crash_recovery_for_uncommitted_data(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', [('name', 'STR(20)'), ('age', 'int')])
    tm.commit(txn1)
    txn2 = tm.begin()
    rse.insert_row(txn2, 'users', ('dave', 40))
    # 不提交txn2，直接模拟崩溃
    # 重启
    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=str(rse.data_dir))
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    txn3 = tm.begin()
    results = list(rse2.scan(txn3, 'users'))
    tm.commit(txn3)
    assert all(row[1] != ('dave', 40) for row in results) 

# 用例6：崩溃恢复-已提交的更新
def test_crash_recovery_for_committed_update(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    # 1. 创建表并插入一行
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', [('name', 'STR(20)'), ('age', 'int')])
    tm.commit(txn1)
    txn2 = tm.begin()
    row_id = rse.insert_row(txn2, 'users', ('frank', 20))
    tm.commit(txn2)
    # 2. 更新该行并提交
    txn3 = tm.begin()
    rse.update_row(txn3, 'users', row_id, ('frank', 21))
    tm.commit(txn3)
    # 3. 模拟崩溃并重启
    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=str(rse.data_dir))
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    # 4. 检查更新是否生效
    txn4 = tm.begin()
    results = list(rse2.scan(txn4, 'users'))
    tm.commit(txn4)
    logger.debug(results)
    assert any(row[1] == ('frank', 21) for row in results)

# 用例7：崩溃恢复-未提交的更新
def test_crash_recovery_for_uncommitted_update(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    # 1. 创建表并插入一行
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', [('name', 'STR(20)'), ('age', 'int')])
    tm.commit(txn1)
    txn2 = tm.begin()
    row_id = rse.insert_row(txn2, 'users', ('grace', 22))
    tm.commit(txn2)
    # 2. 更新该行但不提交
    txn3 = tm.begin()
    rse.update_row(txn3, 'users', row_id, ('grace', 23))
    # 不提交txn3，直接模拟崩溃
    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=str(rse.data_dir))
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    # 3. 检查更新未生效
    txn4 = tm.begin()
    results = list(rse2.scan(txn4, 'users'))
    tm.commit(txn4)
    assert all(row[1] != ('grace', 23) for row in results)

# 用例8：崩溃恢复-已提交的删除

def test_crash_recovery_for_committed_delete(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    # 1. 创建表并插入一行
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', [('name', 'STR(20)'), ('age', 'int')])
    tm.commit(txn1)
    txn2 = tm.begin()
    row_id = rse.insert_row(txn2, 'users', ('helen', 30))
    tm.commit(txn2)
    # 2. 删除该行并提交
    txn3 = tm.begin()
    rse.delete_row(txn3, 'users', row_id)
    tm.commit(txn3)
    # 3. 模拟崩溃并重启
    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=str(rse.data_dir))
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    # 4. 检查删除是否生效
    txn4 = tm.begin()
    results = list(rse2.scan(txn4, 'users'))
    tm.commit(txn4)
    logger.debug(results)
    assert all(row[1] != ('helen', 30) for row in results)

# 用例9：索引创建与崩溃恢复

def test_crash_recovery_for_index_creation(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    # 1. 创建表并插入多行
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', [('name', 'STR(20)'), ('age', 'int')])
    tm.commit(txn1)
    txn2 = tm.begin()
    row_id1 = rse.insert_row(txn2, 'users', ('isaac', 18))
    row_id2 = rse.insert_row(txn2, 'users', ('jane', 22))
    row_id3 = rse.insert_row(txn2, 'users', ('kate', 30))
    tm.commit(txn2)
    # 2. 创建索引
    txn3 = tm.begin()
    rse.create_index(txn3, 'users', 'idx_name', ['name'], [0], is_unique=True)
    tm.commit(txn3)
    # 3. 验证索引能查找数据
    txn4 = tm.begin()
    found_row_id = rse.find_by_index(txn4, 'users', 'idx_name', ('jane',))
    tm.commit(txn4)
    assert found_row_id is not None
    
    # 4. 模拟崩溃并重启
    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=str(rse.data_dir))
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    # 5. 恢复后验证索引依然存在且可用
    table_info = catalog2.get_table_info('users')
    assert 'idx_name' in table_info.indexes
    

    txn5 = tm.begin()
    found_row_id2 = rse2.find_by_index(txn5, 'users', 'idx_name', ('jane',))
    tm.commit(txn5)

    assert found_row_id2 is not None

    # 6. 通过索引查到的row_id能查到正确数据
    txn4 = tm.begin()
    results = dict(rse2.scan(txn4, 'users'))
    tm.commit(txn4)
    assert results[found_row_id2] == ('jane', 22)

# 用例10：索引删除与崩溃恢复

def test_crash_recovery_for_index_drop(setup_env, tm):
    rse, log_manager, lock_manager, catalog = setup_env
    # 1. 创建表并插入多行
    txn1 = tm.begin()
    rse.create_table(txn1, 'users', [('name', 'STR(20)'), ('age', 'int')])
    tm.commit(txn1)
    txn2 = tm.begin()
    row_id1 = rse.insert_row(txn2, 'users', ('isaac', 18))
    row_id2 = rse.insert_row(txn2, 'users', ('jane', 22))
    row_id3 = rse.insert_row(txn2, 'users', ('kate', 30))
    tm.commit(txn2)
    # 2. 创建索引
    txn3 = tm.begin()
    rse.create_index(txn3, 'users', 'idx_name', ['name'], [0], is_unique=True)
    tm.commit(txn3)
    # 3. 删除索引
    txn4 = tm.begin()
    rse.drop_index(txn4, 'users', 'idx_name')
    tm.commit(txn4)
    # 4. 模拟崩溃并重启
    catalog2 = CatalogManager()
    lock_manager2 = LockManager()
    log_manager2 = LogManager(log_manager._log_file_path, None)
    rse2 = RealStorageEngine(catalog2, log_manager2, lock_manager2, data_dir=str(rse.data_dir))
    log_manager2._storage_engine = rse2
    log_manager2.recover()
    # 5. 恢复后验证索引已被删除
    table_info = catalog2.get_table_info('users')
    assert 'idx_name' not in table_info.indexes
    # 6. 索引查找应抛出异常
    txn5 = tm.begin()
    with pytest.raises(Exception):
        rse2.find_by_index(txn5, 'users', 'idx_name', ('jane',))
    tm.commit(txn5)

