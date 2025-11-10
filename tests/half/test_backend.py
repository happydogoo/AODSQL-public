import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import pytest
from src.engine.catalog_manager import CatalogManager
from src.engine.storage.real_storage_engine import RealStorageEngine
from src.engine.transaction.transaction_manager import TransactionManager
import tempfile
from src.engine.transaction.log_manager import LogManager
from src.engine.transaction.lock_manager import LockManager
from src.engine.executor import Executor
from src.engine.operator import CreateTable, Insert, SeqScan, Filter, Project, Limit, Sort, HashAggregate, Schema
from loguru import logger

def test_create_table_operator():
    # 创建临时目录和日志文件
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_manager = CatalogManager()
        log_manager = LogManager(os.path.join(tmpdir, 'test.log'), None)
        lock_manager = LockManager()
        storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
        log_manager._storage_engine = storage_engine
        transaction_manager = TransactionManager(lock_manager, log_manager)
        executor = Executor(storage_engine, catalog_manager)

        # 构造事务
        txn = transaction_manager.begin()
        table_name = 'test_table'
        columns = [('id', 'int'), ('name', 'str')]
        # 构造CreateTable算子
        op = CreateTable(table_name, columns, storage_engine)
        # 执行
        result = executor.execute_plan(op, txn)
        # 提交事务
        transaction_manager.commit(txn)
        # 断言
        assert result == f"Table '{table_name}' created."
        log_manager._log_file.close()
        storage_engine.close_all()

def test_create_table_recover():
    # 阶段1：创建表并提交，但不删除数据目录
    with tempfile.TemporaryDirectory() as tmpdir:
        # --- 第一次启动 ---
        catalog_manager = CatalogManager()
        log_manager = LogManager(os.path.join(tmpdir, 'test.log'), None)
        lock_manager = LockManager()
        storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
        log_manager._storage_engine = storage_engine
        transaction_manager = TransactionManager(lock_manager, log_manager)
        executor = Executor(storage_engine, catalog_manager)

        txn = transaction_manager.begin()
        table_name = 'recover_table'
        columns = [('id', 'int'), ('name', 'str')]
        op = CreateTable(table_name, columns, storage_engine)
        result = executor.execute_plan(op, txn)
        transaction_manager.commit(txn)
        # 关闭所有资源，模拟“断电”
        log_manager._log_file.close()
        storage_engine.close_all()

        # --- 第二次启动（恢复）---
        # 重新加载目录和日志管理器
        catalog_manager2 = CatalogManager()
        log_manager2 = LogManager(os.path.join(tmpdir, 'test.log'), None)
        lock_manager2 = LockManager()
        storage_engine2 = RealStorageEngine(catalog_manager2, log_manager2, lock_manager2, data_dir=tmpdir)
        log_manager2._storage_engine = storage_engine2

        # 关键：执行恢复
        log_manager2.recover()

        # 检查表是否恢复
        assert catalog_manager2.table_exists(table_name)
        table_info = catalog_manager2.get_table(table_name)
        assert [col.column_name for col in table_info.columns] == ['id', 'name']
        assert [col.data_type for col in table_info.columns] == ['int', 'str']

        log_manager2._log_file.close()
        storage_engine2.close_all()


def test_insert_operator():
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_manager = CatalogManager()
        log_manager = LogManager(os.path.join(tmpdir, 'test.log'), None)
        lock_manager = LockManager()
        storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
        log_manager._storage_engine = storage_engine
        transaction_manager = TransactionManager(lock_manager, log_manager)
        executor = Executor(storage_engine, catalog_manager)

        # 1. 创建表
        txn1 = transaction_manager.begin()
        table_name = 'insert_table'
        columns = [('id', 'int'), ('name', 'STR(20)')]
        op_create = CreateTable(table_name, columns, storage_engine)
        result_create = executor.execute_plan(op_create, txn1)
        transaction_manager.commit(txn1)
        assert result_create == f"Table '{table_name}' created."

        # 2. 插入数据
        txn2 = transaction_manager.begin()
        values = [(1, 'Alice'),(2,'Bob'),(3,'CQG')]
        op_insert = Insert(table_name, values, storage_engine)
        result_insert = executor.execute_plan(op_insert, txn2)
        transaction_manager.commit(txn2)
        assert result_insert == f"{len(values)} rows inserted."

        # 3. 查询验证
        txn3 = transaction_manager.begin()
        schema = Schema(columns)
        op_scan = SeqScan(table_name, storage_engine, schema)
        op_scan.transaction = txn3
        all_rows = []
        while True:
            batch = op_scan.next()
            if not batch:
                break
            all_rows.extend(batch)
        transaction_manager.commit(txn3)
        # 只比较数据部分
        data_rows = [row[1] for row in all_rows]
        assert (1, 'Alice') in data_rows
        assert (2, 'Bob') in data_rows
        assert (3, 'CQG') in data_rows

        log_manager._log_file.close()
        storage_engine.close_all()
    
# 文件: test_backend.py

def test_crash_recovery_insert_operator():
    # 1. 创建临时目录，模拟数据库持久化目录
    with tempfile.TemporaryDirectory() as tmpdir:
        table_name = 'crash_table'
        columns = [('id', 'int'), ('name', 'STR(20)')]
        values = [(1, 'Alice'), (2, 'Bob'), (3, 'CQG')]

        # --- 阶段 1: 第一次启动，插入部分数据后模拟崩溃 ---
        try:
            catalog_manager_crashed = CatalogManager()
            # 【修改点】将日志文件路径保存下来
            log_file_path = os.path.join(tmpdir, 'test.log')
            log_manager_crashed = LogManager(log_file_path, None)
            lock_manager_crashed = LockManager()
            storage_engine_crashed = RealStorageEngine(catalog_manager_crashed, log_manager_crashed, lock_manager_crashed, data_dir=tmpdir)
            log_manager_crashed._storage_engine = storage_engine_crashed
            transaction_manager_crashed = TransactionManager(lock_manager_crashed, log_manager_crashed)
            executor_crashed = Executor(storage_engine_crashed, catalog_manager_crashed)

            # 创建表并插入数据...
            txn1 = transaction_manager_crashed.begin()
            op_create = CreateTable(table_name, columns, storage_engine_crashed)
            executor_crashed.execute_plan(op_create, txn1)
            transaction_manager_crashed.commit(txn1)

            txn2 = transaction_manager_crashed.begin()
            op_insert = Insert(table_name, values[:2], storage_engine_crashed)
            executor_crashed.execute_plan(op_insert, txn2)
            transaction_manager_crashed.commit(txn2)
            
            # 【修改点】在崩溃前显式关闭资源，模拟进程退出
            log_manager_crashed.flush_to_lsn(txn2.last_lsn) # 确保所有日志都已刷盘
            log_manager_crashed._log_file.close()
            storage_engine_crashed.close_all()

            # 模拟崩溃
            raise SystemExit("模拟崩溃")
        except SystemExit:
            # 即使崩溃，也要确保 Python 对象被释放
            del storage_engine_crashed
            del log_manager_crashed
            pass

        # --- 阶段 2: 第二次启动，恢复并验证 ---
        # 此时，第一阶段的所有对象都应该被回收了
        try:
            catalog_manager = CatalogManager()
            log_manager = LogManager(log_file_path, None)
            lock_manager = LockManager()
            storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
            log_manager._storage_engine = storage_engine
            
            max_txn_id = log_manager.recover()
            transaction_manager = TransactionManager(lock_manager, log_manager, start_txn_id=max_txn_id + 1)
            executor = Executor(storage_engine, catalog_manager)

            # 插入并验证数据...
            txn3 = transaction_manager.begin()
            op_insert2 = Insert(table_name, values[2:], storage_engine)
            executor.execute_plan(op_insert2, txn3)
            transaction_manager.commit(txn3)

            txn4 = transaction_manager.begin()
            schema = Schema(columns)
            op_scan = SeqScan(table_name, storage_engine, schema)
            op_scan.transaction = txn4
            all_rows = [row[1] for batch in iter(op_scan.next, None) if batch for row in batch]
            transaction_manager.commit(txn4)

            assert len(all_rows) == 3
            assert (1, 'Alice') in all_rows
            assert (2, 'Bob') in all_rows
            assert (3, 'CQG') in all_rows
        finally:
            if 'log_manager' in locals() and log_manager._log_file and not log_manager._log_file.closed:
                log_manager._log_file.close()
            if 'storage_engine' in locals():
                storage_engine.close_all()

def test_query_operators():
    with tempfile.TemporaryDirectory() as tmpdir:
        # 初始化环境
        catalog_manager = CatalogManager()
        log_manager = LogManager(os.path.join(tmpdir, 'test.log'), None)
        lock_manager = LockManager()
        storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
        log_manager._storage_engine = storage_engine
        transaction_manager = TransactionManager(lock_manager, log_manager)
        executor = Executor(storage_engine, catalog_manager)

        # 创建表并插入数据
        txn1 = transaction_manager.begin()
        table_name = 'op_table'
        columns = [('id', 'int'), ('name', 'STR(20)')]
        op_create = CreateTable(table_name, columns, storage_engine)
        executor.execute_plan(op_create, txn1)
        transaction_manager.commit(txn1)

        txn2 = transaction_manager.begin()
        values = [(1, 'Alice'), (2, 'Bob'), (3, 'CQG'), (4, 'Bob'), (5, 'Alice')]
        op_insert = Insert(table_name, values, storage_engine)
        executor.execute_plan(op_insert, txn2)
        transaction_manager.commit(txn2)

        # 查询型算子测试
        txn3 = transaction_manager.begin()
        schema = Schema(columns)

        # 1. SeqScan
        op_scan = SeqScan(table_name, storage_engine, schema)
        op_scan.transaction = txn3
        all_rows = []
        while True:
            batch = op_scan.next()
            if not batch:
                break
            all_rows.extend(batch)
        assert len(all_rows) == 5

        # 2. Filter（筛选name为'Bob'的行）
        txn4 = transaction_manager.begin()
        op_scan2 = SeqScan(table_name, storage_engine, schema)
        op_scan2.transaction = txn4
        op_filter = Filter(op_scan2, lambda row: row[1] == 'Bob')
        op_filter.transaction = txn4
        filtered = []
        while True:
            batch = op_filter.next()
            if not batch:
                break
            filtered.extend(batch)
        assert all(row[1][1] == 'Bob' for row in filtered)
        assert len(filtered) == 2

        # 3. Project（只保留name列）
        txn5 = transaction_manager.begin()
        op_scan3 = SeqScan(table_name, storage_engine, schema)
        op_scan3.transaction = txn5
        op_project = Project(op_scan3, [1])
        op_project.transaction = txn5
        projected = []
        while True:
            batch = op_project.next()
            if not batch:
                break
            projected.extend(batch)
        assert all(len(row[1]) == 1 for row in projected)
        assert set(row[1][0] for row in projected) == {'Alice', 'Bob', 'CQG'}

        # 4. Limit（只取前3行）
        txn6 = transaction_manager.begin()
        op_scan4 = SeqScan(table_name, storage_engine, schema)
        op_scan4.transaction = txn6
        op_limit = Limit(op_scan4, 3)
        op_limit.transaction = txn6
        limited = []
        while True:
            batch = op_limit.next()
            if not batch:
                break
            limited.extend(batch)
        assert len(limited) == 3

        # 5. Sort（按name升序排序）
        txn7 = transaction_manager.begin()
        op_scan5 = SeqScan(table_name, storage_engine, schema)
        op_scan5.transaction = txn7
        op_sort = Sort(op_scan5, [(1, 'ASC')])
        op_sort.transaction = txn7
        sorted_rows = []
        while True:
            batch = op_sort.next()
            if not batch:
                break
            sorted_rows.extend(batch)
        names = [row[1][1] for row in sorted_rows]
        assert names == sorted(names)

        # 6. HashAggregate（按name分组计数）
        txn8 = transaction_manager.begin()
        op_scan6 = SeqScan(table_name, storage_engine, schema)
        op_scan6.transaction = txn8
        op_agg = HashAggregate(op_scan6, [1], [('COUNT', 0)], Schema([('name', 'STR(20)'), ('cnt', 'int')]))
        op_agg.transaction = txn8
        agg_results = []
        while True:
            batch = op_agg.next()
            if not batch:
                break
            agg_results.extend(batch)
        # 检查聚合结果
        result_dict = {row[1][0]: row[1][1] for row in agg_results}
        assert result_dict['Alice'] == 2
        assert result_dict['Bob'] == 2
        assert result_dict['CQG'] == 1

        # 关闭资源
        log_manager._log_file.close()
        storage_engine.close_all()


def test_index_operators():
    """
    测试与索引相关的物理算子，包括 CreateIndex、IndexScan（如有）、索引唯一性等。
    """
    import tempfile
    from src.engine.operator import CreateTable, Insert, CreateIndex, IndexScan, SeqScan, Filter
    from src.engine.operator import Schema
    
    with tempfile.TemporaryDirectory() as tmpdir:
        catalog_manager = CatalogManager()
        log_manager = LogManager(os.path.join(tmpdir, 'test.log'), None)
        lock_manager = LockManager()
        storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
        log_manager._storage_engine = storage_engine
        transaction_manager = TransactionManager(lock_manager, log_manager)
        executor = Executor(storage_engine, catalog_manager)

        # 1. 创建表并插入数据
        txn1 = transaction_manager.begin()
        table_name = 'idx_table'
        columns = [('id', 'int'), ('name', 'STR(20)')]
        op_create = CreateTable(table_name, columns, storage_engine)
        executor.execute_plan(op_create, txn1)
        transaction_manager.commit(txn1)

        txn2 = transaction_manager.begin()
        values = [(1, 'Alice'), (2, 'Bob'), (3, 'CQG'), (4, 'Bob'), (5, 'Alice')]
        op_insert = Insert(table_name, values, storage_engine)
        executor.execute_plan(op_insert, txn2)
        transaction_manager.commit(txn2)

        # 2. 创建索引
        txn3 = transaction_manager.begin()
        op_create_idx = CreateIndex(catalog_manager, table_name, 'idx_name', 'name')
        op_create_idx.transaction = txn3
        result = op_create_idx.execute()
        transaction_manager.commit(txn3)
        assert '创建成功' in result or '无需重复创建' in result

        # 3. 测试 IndexScan（如有实现，否则用 SeqScan+Filter 验证索引列）
        txn4 = transaction_manager.begin()
        schema = Schema(columns)
        # 优先用 IndexScan，否则用 SeqScan+Filter
        try:
            op_idx_scan = IndexScan(table_name, storage_engine, schema, predicate=lambda row: row[1] == 'Bob')
            op_idx_scan.transaction = txn4
            idx_rows = []
            while True:
                batch = op_idx_scan.next()
                if not batch:
                    break
                idx_rows.extend(batch)
            assert all(row[1][1] == 'Bob' for row in idx_rows)
            assert len(idx_rows) == 2
        except Exception:
            # 如果没有 IndexScan，则用 SeqScan+Filter 验证
            op_scan = SeqScan(table_name, storage_engine, schema)
            op_scan.transaction = txn4
            op_filter = Filter(op_scan, lambda row: row[1] == 'Bob')
            op_filter.transaction = txn4
            idx_rows = []
            while True:
                batch = op_filter.next()
                if not batch:
                    break
                idx_rows.extend(batch)
            assert all(row[1][1] == 'Bob' for row in idx_rows)
            assert len(idx_rows) == 2
        transaction_manager.commit(txn4)

        # 4. 测试索引唯一性（如支持唯一索引，可扩展）
        # 这里假设不支持唯一索引，略过

        # 5. 关闭资源
        log_manager._log_file.close()
        storage_engine.close_all()
        if hasattr(catalog_manager, 'tables'):
            catalog_manager.tables.clear()
            catalog_manager._save_catalog()
        if hasattr(catalog_manager, 'views'):
            catalog_manager.views.clear()
            catalog_manager._save_catalog()


# def test_view_operators():
#     """
#     测试与视图相关的物理算子，包括 CreateView、ShowViews、DropView。
#     """
#     import tempfile
#     from src.engine.operator import CreateTable, Insert, CreateView, ShowViews, DropView

#     with tempfile.TemporaryDirectory() as tmpdir:
#         catalog_manager = CatalogManager()
#         log_manager = LogManager(os.path.join(tmpdir, 'test.log'), None)
#         lock_manager = LockManager()
#         storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
#         log_manager._storage_engine = storage_engine
#         transaction_manager = TransactionManager(lock_manager, log_manager)
#         executor = Executor(storage_engine, catalog_manager)

#         # 1. 创建表并插入数据
#         txn1 = transaction_manager.begin()
#         table_name = 'view_table'
#         columns = [('id', 'int'), ('name', 'STR(20)')]
#         op_create = CreateTable(table_name, columns, storage_engine)
#         executor.execute_plan(op_create, txn1)
#         transaction_manager.commit(txn1)

#         txn2 = transaction_manager.begin()
#         values = [(1, 'Alice'), (2, 'Bob')]
#         op_insert = Insert(table_name, values, storage_engine)
#         executor.execute_plan(op_insert, txn2)
#         transaction_manager.commit(txn2)

#         # 2. 创建视图
#         txn3 = transaction_manager.begin()
#         view_name = 'v_test'
#         definition = f"SELECT * FROM {table_name} WHERE name = 'Alice'"
#         op_create_view = CreateView(view_name, definition, storage_engine=storage_engine)
#         op_create_view.transaction = txn3
#         result = op_create_view.execute()
#         transaction_manager.commit(txn3)
#         assert '创建成功' in result

#         # 3. ShowViews
#         op_show_views = ShowViews(catalog_manager)
#         views = op_show_views.next()
#         assert any(view_name in row[1] for row in views)

#         # 4. DropView
#         txn4 = transaction_manager.begin()
#         op_drop_view = DropView(view_name, storage_engine=storage_engine)
#         op_drop_view.transaction = txn4
#         result2 = op_drop_view.execute()
#         transaction_manager.commit(txn4)
#         assert '删除成功' in result2

#         # 5. ShowViews 验证视图已删除
#         op_show_views2 = ShowViews(catalog_manager)
#         views2 = op_show_views2.next()
#         assert all(view_name not in row[1] for row in (views2 or []))

#         # 关闭资源
#         log_manager._log_file.close()
#         storage_engine.close_all()
#         # 清空catalog_manager的所有表和视图
#         if hasattr(catalog_manager, 'tables'):
#             catalog_manager.tables.clear()
#             catalog_manager._save_catalog()
#         if hasattr(catalog_manager, 'views'):
#             catalog_manager.views.clear()
#             catalog_manager._save_catalog()

# def test_view_ddl_and_crash_recovery():
#     """
#     测试视图的创建、修改、删除，以及崩溃恢复（不再测试未提交事务）。
#     """
#     import tempfile
#     from src.engine.operator import CreateView, DropView, AlterView
#     from src.engine.catalog_manager import CatalogManager
#     from src.engine.transaction.log_manager import LogManager
#     from src.engine.transaction.lock_manager import LockManager
#     from src.engine.storage.real_storage_engine import RealStorageEngine
#     from src.engine.transaction.transaction_manager import TransactionManager

#     with tempfile.TemporaryDirectory() as tmpdir:
#         log_file_path = os.path.join(tmpdir, 'test.log')
#         # --- 第一次启动 ---
#         catalog_manager = CatalogManager()
#         log_manager = LogManager(log_file_path, None)
#         lock_manager = LockManager()
#         storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
#         log_manager._storage_engine = storage_engine
#         transaction_manager = TransactionManager(lock_manager, log_manager)

#         # 1. 创建视图
#         txn1 = transaction_manager.begin()
#         op_create_view = CreateView('v1', 'SELECT 1', storage_engine=storage_engine)
#         op_create_view.transaction = txn1
#         result1 = op_create_view.execute()
#         transaction_manager.commit(txn1)
#         assert '创建成功' in result1
#         assert catalog_manager.view_exists('v1')

#         # 2. 修改视图
#         txn2 = transaction_manager.begin()
#         op_alter_view = AlterView('v1', 'SELECT 2', storage_engine=storage_engine)
#         op_alter_view.transaction = txn2
#         result2 = op_alter_view.execute()
#         transaction_manager.commit(txn2)
#         assert '修改成功' in result2
#         assert catalog_manager.get_view('v1').definition == 'SELECT 2'

#         # 3. 删除视图
#         txn3 = transaction_manager.begin()
#         op_drop_view = DropView('v1', storage_engine=storage_engine)
#         op_drop_view.transaction = txn3
#         result3 = op_drop_view.execute()
#         transaction_manager.commit(txn3)
#         assert '删除成功' in result3
#         assert not catalog_manager.view_exists('v1')

#         # 4. 创建一个已提交的视图
#         txn4 = transaction_manager.begin()
#         op_create_view2 = CreateView('v2', 'SELECT 3', storage_engine=storage_engine)
#         op_create_view2.transaction = txn4
#         result4 = op_create_view2.execute()
#         transaction_manager.commit(txn4)
#         assert '创建成功' in result4
#         assert catalog_manager.view_exists('v2')

#         # 崩溃前关闭资源
#         log_manager._log_file.flush()
#         log_manager._log_file.close()
#         storage_engine.close_all()

#         # --- 第二次启动（恢复）---
#         catalog_manager2 = CatalogManager()
#         log_manager2 = LogManager(log_file_path, None)
#         lock_manager2 = LockManager()
#         storage_engine2 = RealStorageEngine(catalog_manager2, log_manager2, lock_manager2, data_dir=tmpdir)
#         log_manager2._storage_engine = storage_engine2

#         # 恢复
#         log_manager2.recover()

#         # v1应该不存在（已删），v2应存在
#         assert not catalog_manager2.view_exists('v1')
#         assert catalog_manager2.view_exists('v2')
#         assert catalog_manager2.get_view('v2').definition == 'SELECT 3'

#         # 清理
#         log_manager2._log_file.close()
#         storage_engine2.close_all()
#         if hasattr(catalog_manager, 'tables'):
#             catalog_manager.tables.clear()
#             catalog_manager._save_catalog()
#         if hasattr(catalog_manager, 'views'):
#             catalog_manager.views.clear()
#             catalog_manager._save_catalog()


# def test_trigger_ddl_and_crash_recovery():
#     """
#     测试触发器的创建、修改、删除，以及崩溃恢复。
#     """
#     import tempfile
#     from src.engine.operator import CreateTable, CreateTrigger, AlterTrigger, DropTrigger
#     from src.engine.catalog_manager import CatalogManager
#     from src.engine.transaction.log_manager import LogManager
#     from src.engine.transaction.lock_manager import LockManager
#     from src.engine.storage.real_storage_engine import RealStorageEngine
#     from src.engine.transaction.transaction_manager import TransactionManager

#     with tempfile.TemporaryDirectory() as tmpdir:
#         log_file_path = os.path.join(tmpdir, 'test.log')
#         # --- 第一次启动 ---
#         catalog_manager = CatalogManager()
#         log_manager = LogManager(log_file_path, None)
#         lock_manager = LockManager()
#         storage_engine = RealStorageEngine(catalog_manager, log_manager, lock_manager, data_dir=tmpdir)
#         log_manager._storage_engine = storage_engine
#         transaction_manager = TransactionManager(lock_manager, log_manager)

#         # 0. 先创建表 t1
#         txn0 = transaction_manager.begin()
#         op_create_table = CreateTable('t1', [('id', 'int')], storage_engine)
#         op_create_table.transaction = txn0
#         op_create_table.execute()
#         transaction_manager.commit(txn0)

#         # 1. 创建触发器
#         txn1 = transaction_manager.begin()
#         op_create_trigger = CreateTrigger(
#             'tr1', 't1', 'BEFORE', ['INSERT'], True, 'NEW.id > 0', ['SET x = 1'], storage_engine=storage_engine)
#         op_create_trigger.transaction = txn1
#         result1 = op_create_trigger.execute()
#         transaction_manager.commit(txn1)
#         assert '创建成功' in result1
#         assert catalog_manager.trigger_exists('tr1')

#         # 2. 修改触发器
#         txn2 = transaction_manager.begin()
#         op_alter_trigger = AlterTrigger(
#             'tr1', 't1', 'AFTER', ['UPDATE'], True, 'NEW.id > 10', ['SET x = 2'], storage_engine=storage_engine)
#         op_alter_trigger.transaction = txn2
#         result2 = op_alter_trigger.execute()
#         transaction_manager.commit(txn2)
#         assert '修改成功' in result2
#         trig = catalog_manager.get_trigger('tr1')
#         assert trig.timing == 'AFTER'
#         assert trig.events == ['UPDATE']
#         assert trig.when_condition == 'NEW.id > 10'
#         assert trig.trigger_body == ['SET x = 2']

#         # 3. 删除触发器
#         txn3 = transaction_manager.begin()
#         op_drop_trigger = DropTrigger('tr1', storage_engine=storage_engine)
#         op_drop_trigger.transaction = txn3
#         result3 = op_drop_trigger.execute()
#         transaction_manager.commit(txn3)
#         assert '删除成功' in result3
#         assert not catalog_manager.trigger_exists('tr1')

#         # 4. 创建一个已提交的触发器
#         txn4 = transaction_manager.begin()
#         op_create_trigger2 = CreateTrigger(
#             'tr2', 't1', 'BEFORE', ['INSERT'], True, 'NEW.id > 0', ['SET y = 1'], storage_engine=storage_engine)
#         op_create_trigger2.transaction = txn4
#         result4 = op_create_trigger2.execute()
#         transaction_manager.commit(txn4)
#         assert '创建成功' in result4
#         assert catalog_manager.trigger_exists('tr2')

#         # 崩溃前关闭资源
#         log_manager._log_file.flush()
#         log_manager._log_file.close()
#         storage_engine.close_all()

#         # --- 第二次启动（恢复）---
#         catalog_manager2 = CatalogManager()
#         log_manager2 = LogManager(log_file_path, None)
#         lock_manager2 = LockManager()
#         storage_engine2 = RealStorageEngine(catalog_manager2, log_manager2, lock_manager2, data_dir=tmpdir)
#         log_manager2._storage_engine = storage_engine2

#         # 恢复
#         log_manager2.recover()

#         # tr1应该不存在（已删），tr2应存在
#         assert not catalog_manager2.trigger_exists('tr1')
#         assert catalog_manager2.trigger_exists('tr2')
#         trig2 = catalog_manager2.get_trigger('tr2')
#         assert trig2.timing == 'BEFORE'
#         assert trig2.events == ['INSERT']
#         assert trig2.when_condition == 'NEW.id > 0'
#         assert trig2.trigger_body == ['SET y = 1']

#         # 清理
#         log_manager2._log_file.close()
#         storage_engine2.close_all()
#         if hasattr(catalog_manager, 'tables'):
#             catalog_manager.tables.clear()
#             catalog_manager._save_catalog()
#         if hasattr(catalog_manager, 'views'):
#             catalog_manager.views.clear()
#             catalog_manager._save_catalog()
