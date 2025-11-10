import pytest
from src.engine.executor import Executor
from src.engine.catalog_manager import CatalogManager
from src.engine.storage.storage_engine import StorageEngine
from src.engine.trigger.trigger_manager import TriggerManager
from src.engine.trigger.trigger_executor import TriggerExecutor
from src.engine.operator import Operator
from src.engine.transaction.transaction import Transaction
from unittest.mock import MagicMock

class DummyOperator(Operator):
    def __init__(self):
        super().__init__()
        self.executed = False
        self.child = None
        self.left_child = None
        self.right_child = None
    def execute(self):
        self.executed = True
        return 'executed'
    def next(self):
        return [(1, (1,))]





def test_execute_trigger_statement():
    executor = Executor(MagicMock(), MagicMock())
    ast = MagicMock()
    # 不支持类型
    res = executor.execute_trigger_statement(ast)
    assert not res['success']


def test_fire_triggers():
    executor = Executor(MagicMock(), MagicMock())
    # 只要能调用到trigger_executor即可
    executor.trigger_executor = MagicMock()
    executor.trigger_executor.fire_triggers_for_insert.return_value = True
    assert executor.fire_triggers_for_insert('t', {'id': 1})
    executor.trigger_executor.fire_triggers_for_update.return_value = True
    assert executor.fire_triggers_for_update('t', {'id': 1}, {'id': 2})
    executor.trigger_executor.fire_triggers_for_delete.return_value = True
    assert executor.fire_triggers_for_delete('t', {'id': 1})
