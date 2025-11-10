import unittest
import os
import shutil
import random
from unittest.mock import patch
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入项目结构中所有必要的组件
# 注意：如果你的项目结构不同，请调整导入路径。
from src.engine.storage.real_storage_engine import RealStorageEngine
from src.engine.catalog_manager import CatalogManager
from src.engine.transaction.log_manager import LogManager
from src.engine.transaction.lock_manager import LockManager
from src.engine.transaction.transaction import Transaction
from src.engine.transaction.transaction_manager import TransactionManager

from cli.plan_converter import PlanConverter
from src.engine.executor import Executor
from src.engine.operator import SeqScan # We will mock this class

# 用于测试的简单事务管理器
class MockTransactionManager:
    def __init__(self):
        self.next_txn_id = 0
    def begin(self):
        self.next_txn_id += 1
        return Transaction(self.next_txn_id)

class TestIndexScanFunctionality(unittest.TestCase):
    """
    集成测试：验证 IndexScan 算子是否能正确工作。
    """
    def setUp(self):
        """
        在每个测试前设置临时数据库环境。
        包括创建表、插入数据、建立索引等。
        """
        self.test_dir = 'test_db_data'
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)

        # 1. 初始化所有数据库组件
        self.catalog_manager = CatalogManager()
        self.lock_manager = LockManager()
        # 测试时可用一个虚拟 LogManager（如果不测试恢复功能）
        self.log_manager = LogManager(log_file_path=os.path.join(self.test_dir, 'test.log'))
        
        self.storage_engine = RealStorageEngine(
            catalog_manager=self.catalog_manager,
            log_manager=self.log_manager,
            lock_manager=self.lock_manager,
            data_dir=self.test_dir
        )
        self.txn_manager = MockTransactionManager()
        self.executor = Executor(self.storage_engine, self.catalog_manager)
        self.plan_converter = PlanConverter(self.storage_engine, self.catalog_manager)

        # 2. 创建表和索引
        self.table_name = "users"
        self.index_name = "idx_users_id"
        self.schema = [("id", "INT"), ("name", "VARCHAR"), ("age", "INT")]
        
        txn = self.txn_manager.begin()  # 启动事务
        # 创建表
        self.storage_engine.create_table(txn, self.table_name, self.schema)
        # 创建索引
        self.storage_engine.create_index(
            transaction=txn,
            table_name=self.table_name,
            index_name=self.index_name,
            key_columns=["id"],
            key_col_types=[1], # 1 for INT
            is_unique=True
        )

        # 3. 向表中插入200条记录
        self.test_data = []
        for i in range(1, 201):
            row = (i, f"user_{i}", 20 + (i % 50))
            self.test_data.append(row)
            self.storage_engine.insert_row(txn, self.table_name, row)
        
        # 我们将在数据集中间查找一个值
        self.search_id = 157
        self.expected_row = next(row for row in self.test_data if row[0] == self.search_id)


    def tearDown(self):
        """
        每个测试后清理数据库环境。
        """
        self.storage_engine.close_all()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_index_scan_returns_correct_data(self):
        """
        测试1：正确性。
        验证使用 IndexScan 的查询与使用全表 SeqScan+Filter 的查询结果完全一致。
        """
        print("\n--- 正在运行正确性测试 ---")
        
        # --- 准备阶段 ---
        # A. 构造索引扫描的逻辑计划
        logical_plan_index = {
            "type": "INDEX_SCAN",
            "properties": {
                "table_name": self.table_name,
                "index_name": self.index_name,
                "predicate": f"id = {self.search_id}" # The condition that the optimizer would extract
            }
        }

        # B. 构造顺序扫描+过滤的逻辑计划
        logical_plan_seq = {
            "type": "FILTER",
            "properties": {"condition": f"id = {self.search_id}"},
            "children": [{
                "type": "SCAN",
                "properties": {"table_name": self.table_name}
            }]
        }

        # --- 执行阶段 ---
        # A. 执行索引扫描计划
        txn_index = self.txn_manager.begin()
        physical_plan_index = self.plan_converter.convert_to_physical_plan(logical_plan_index)
        index_result = self.executor.execute_plan(physical_plan_index, txn_index)

        # B. 执行顺序扫描计划
        txn_seq = self.txn_manager.begin()
        physical_plan_seq = self.plan_converter.convert_to_physical_plan(logical_plan_seq)
        seq_result = self.executor.execute_plan(physical_plan_seq, txn_seq)

        # --- 断言阶段 ---
        print(f"索引扫描结果: {index_result}")
        print(f"顺序扫描结果: {seq_result}")
        
        # 1. 两种方式都应只返回一行
        self.assertEqual(len(index_result), 1)
        self.assertEqual(len(seq_result), 1)

        # 2. 提取数据元组 (row_id, data_tuple)
        index_row_data = index_result[0][1]
        seq_row_data = seq_result[0][1]
        
        # 3. 两种方式的数据都应与预期行一致
        self.assertEqual(index_row_data, self.expected_row)
        self.assertEqual(seq_row_data, self.expected_row)

        # 4. 两种方式的数据必须完全相同
        self.assertEqual(index_row_data, seq_row_data)
        
        print("✅ 正确性测试通过：IndexScan 与 SeqScan+Filter 返回了完全一致且正确的结果。")

    @patch('src.engine.operator.SeqScan.next')
    def test_index_scan_does_not_use_seq_scan(self, mock_seq_scan_next):
        """
        测试2：原理验证。
        验证当执行 IndexScan 计划时，SeqScan 算子的 next 方法不会被调用。
        这证明了确实走了索引路径。
        """
        print("\n--- 正在运行原理验证测试 ---")
        
        # --- 准备阶段 ---
        # 构造索引扫描的逻辑计划
        logical_plan_index = {
            "type": "INDEX_SCAN",
            "properties": {
                "table_name": self.table_name,
                "index_name": self.index_name,
                "predicate": f"id = {self.search_id}"
            }
        }
        
        # --- 执行阶段 ---
        # 在 mock 有效时执行索引扫描计划
        txn = self.txn_manager.begin()
        physical_plan = self.plan_converter.convert_to_physical_plan(logical_plan_index)
        self.executor.execute_plan(physical_plan, txn)
        
        # --- 断言阶段 ---
        # 检查 SeqScan.next 方法是否被调用。
        # 如果索引扫描生效，则不应被调用。
        mock_seq_scan_next.assert_not_called()
        
        print("✅ 原理验证测试通过：索引查询过程中未使用 SeqScan。")


if __name__ == '__main__':
    unittest.main()