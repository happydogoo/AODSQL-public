import pytest
from unittest.mock import MagicMock
from src.engine.trigger.trigger_executor import TriggerExecutor
from src.engine.trigger.trigger_manager import TriggerManager
from src.engine.catalog_manager import CatalogManager
from src.engine.storage.storage_engine import StorageEngine
from src.sql_compiler.ast_nodes import CreateTriggerStatement, DropTriggerStatement, ShowTriggers

@pytest.fixture
def executor():
    trigger_manager = MagicMock(spec=TriggerManager)
    catalog_manager = MagicMock(spec=CatalogManager)
    storage_engine = MagicMock(spec=StorageEngine)
    return TriggerExecutor(trigger_manager, catalog_manager, storage_engine)

def test_execute_create_trigger_success(executor):
    ast = MagicMock(spec=CreateTriggerStatement)
    ast.table_name = MagicMock()
    ast.table_name.value = 't1'
    executor.catalog_manager.table_exists.return_value = True
    executor._convert_ast_to_trigger_info = MagicMock()
    executor.trigger_manager.create_trigger.return_value = True
    result = executor.execute_create_trigger(ast)
    assert result['success']
    assert '创建成功' in result['message']

def test_execute_create_trigger_table_not_exist(executor):
    ast = MagicMock(spec=CreateTriggerStatement)
    ast.table_name = MagicMock()
    ast.table_name.value = 't2'
    executor.catalog_manager.table_exists.return_value = False
    result = executor.execute_create_trigger(ast)
    assert not result['success']
    assert '不存在' in result['message']

def test_execute_create_trigger_fail(executor):
    ast = MagicMock(spec=CreateTriggerStatement)
    ast.table_name = MagicMock()
    ast.table_name.value = 't3'
    executor.catalog_manager.table_exists.return_value = True
    executor._convert_ast_to_trigger_info = MagicMock()
    executor.trigger_manager.create_trigger.return_value = False
    result = executor.execute_create_trigger(ast)
    assert not result['success']
    assert '创建失败' in result['message']

def test_execute_drop_trigger_success(executor):
    ast = MagicMock(spec=DropTriggerStatement)
    ast.trigger_name = MagicMock()
    ast.trigger_name.value = 'trg1'
    executor.trigger_manager.drop_trigger.return_value = True
    result = executor.execute_drop_trigger(ast)
    assert result['success']
    assert '删除成功' in result['message']

def test_execute_drop_trigger_fail(executor):
    ast = MagicMock(spec=DropTriggerStatement)
    ast.trigger_name = MagicMock()
    ast.trigger_name.value = 'trg2'
    executor.trigger_manager.drop_trigger.return_value = False
    result = executor.execute_drop_trigger(ast)
    assert not result['success']
    assert '不存在' in result['message']

def test_execute_show_triggers(executor):
    ast = MagicMock(spec=ShowTriggers)
    executor.trigger_manager.get_all_triggers.return_value = []
    result = executor.execute_show_triggers(ast)
    assert result['success']
    assert isinstance(result['data'], list)

def test_fire_triggers_for_insert(executor):
    executor.trigger_manager.execute_triggers.return_value = True
    assert executor.fire_triggers_for_insert('t', {'a': 1})

def test_fire_triggers_for_update(executor):
    executor.trigger_manager.execute_triggers.return_value = True
    assert executor.fire_triggers_for_update('t', {'a': 1}, {'a': 2})

def test_fire_triggers_for_delete(executor):
    executor.trigger_manager.execute_triggers.return_value = True
    assert executor.fire_triggers_for_delete('t', {'a': 1}) 