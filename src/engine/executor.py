# -*- coding: utf-8 -*-
"""
执行引擎 (Executor)

职责：
- 接收物理执行计划（算子树根），驱动执行并返回结果
- 为算子树注入事务上下文（Transaction）
- 调用触发器执行器处理触发器相关语句与回调

说明：
- 本模块仅补充文档与类型注解，不改动现有执行逻辑
"""
from src.engine.storage.storage_engine import StorageEngine
from src.engine.catalog_manager import CatalogManager
from src.engine.trigger.trigger_manager import TriggerManager
from src.engine.trigger.trigger_executor import TriggerExecutor
from src.sql_compiler.ast_nodes import CreateTriggerStatement, DropTriggerStatement, ShowTriggers
# from loguru import logger
from src.engine.operator import Operator
# 新增导入
from src.engine.transaction.transaction import Transaction
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class Executor:
    """
    执行器 (Executor)。
    负责接收一个物理计划（算子树的根节点），并驱动其执行，最终返回结果。
    """
    def __init__(
        self,
        storage_engine: StorageEngine,
        catalog_manager: CatalogManager,
        trigger_manager: Optional[TriggerManager] = None,
    ) -> None:
        """
        初始化执行器。

        Args:
            storage_engine: 存储引擎实例。
            catalog_manager: 系统目录实例。
            trigger_manager: 触发器管理器实例，缺省时自动创建。
        """
        self.storage_engine = storage_engine
        self.catalog_manager = catalog_manager
        self.trigger_manager = trigger_manager or TriggerManager()
        self.trigger_executor = TriggerExecutor(self.trigger_manager, catalog_manager, storage_engine)

    # 1. 修改方法签名，增加 transaction 参数
    def execute_plan(self, plan_root: Operator, transaction: Transaction) -> Any:
        """
        驱动物理计划执行的核心方法。

        Args:
            plan_root: 物理计划根节点（算子树）。
            transaction: 事务上下文。

        Returns:
            Any: 针对迭代算子返回结果批次列表，针对终止型算子返回其执行结果。
        """
        # 2. 在执行前，将事务对象注入到整个算子树中
        self._inject_transaction(plan_root, transaction)
        
        # 后续逻辑保持不变
        if type(plan_root).next is not Operator.next:
            results = []
            while True:
                batch = plan_root.next()
                if batch is None or not batch:
                    break
                results.extend(batch)
            return results
        elif type(plan_root).execute is not Operator.execute:
            return plan_root.execute()
        else:
            raise ValueError(f"Unsupported operator type: {type(plan_root).__name__}")

    def _inject_transaction(self, operator: Operator, transaction: Transaction) -> None:
        """
        递归为算子树中的每个节点设置事务上下文。

        Args:
            operator: 当前算子节点。
            transaction: 事务上下文。
        """
        if operator is None:
            return
        # 将“护照”发给当前算子
        operator.transaction = transaction
        # 递归地为所有子节点签发“护照”
        if hasattr(operator, 'child') and getattr(operator, 'child') is not None:
            self._inject_transaction(operator.child, transaction)
        if hasattr(operator, 'left_child') and getattr(operator, 'left_child') is not None:
            self._inject_transaction(operator.left_child, transaction)
        if hasattr(operator, 'right_child') and getattr(operator, 'right_child') is not None:
            self._inject_transaction(operator.right_child, transaction)
    
    def execute_trigger_statement(self, ast) -> Dict[str, Any]:
        """
        执行触发器相关语句。

        Args:
            ast: 触发器语句 AST 节点。

        Returns:
            Dict[str, Any]: 执行结果。
        """
        try:
            if isinstance(ast, CreateTriggerStatement):
                return self.trigger_executor.execute_create_trigger(ast)
            elif isinstance(ast, DropTriggerStatement):
                return self.trigger_executor.execute_drop_trigger(ast)
            elif isinstance(ast, ShowTriggers):
                return self.trigger_executor.execute_show_triggers(ast)
            else:
                return {
                    "success": False,
                    "message": f"不支持的触发器语句类型: {type(ast).__name__}"
                }
        except Exception as e:
            logger.error(f"执行触发器语句失败: {e}")
            return {
                "success": False,
                "message": f"触发器语句执行失败: {str(e)}"
            }
    
    def fire_triggers_for_insert(self, table_name: str, new_data: Dict[str, Any]) -> bool:
        """
        为INSERT操作触发触发器
        
        Args:
            table_name: 表名
            new_data: 新插入的数据
            
        Returns:
            bool: 触发器执行是否成功
        """
        return self.trigger_executor.fire_triggers_for_insert(table_name, new_data)
    
    def fire_triggers_for_update(self, table_name: str, old_data: Dict[str, Any], 
                                new_data: Dict[str, Any]) -> bool:
        """
        为UPDATE操作触发触发器
        
        Args:
            table_name: 表名
            old_data: 更新前的数据
            new_data: 更新后的数据
            
        Returns:
            bool: 触发器执行是否成功
        """
        return self.trigger_executor.fire_triggers_for_update(table_name, old_data, new_data)
    
    def fire_triggers_for_delete(self, table_name: str, old_data: Dict[str, Any]) -> bool:
        """
        为DELETE操作触发触发器
        
        Args:
            table_name: 表名
            old_data: 删除前的数据
            
        Returns:
            bool: 触发器执行是否成功
        """
        return self.trigger_executor.fire_triggers_for_delete(table_name, old_data)
