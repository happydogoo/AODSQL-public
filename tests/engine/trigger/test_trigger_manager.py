import pytest
from src.engine.trigger.trigger_manager import TriggerManager, TriggerInfo, TriggerTiming, TriggerEvent
from unittest.mock import MagicMock
from datetime import datetime

def make_trigger(name='trg', table='t', timing=TriggerTiming.BEFORE, events=None):
    if events is None:
        events = [TriggerEvent.INSERT]
    return TriggerInfo(
        name=name,
        table_name=table,
        timing=timing,
        events=events,
        is_row_level=True,
        when_condition=None,
        trigger_body=[],
        created_at=datetime.now().isoformat()
    )

def test_create_and_get_trigger():
    mgr = TriggerManager()
    trg = make_trigger('trg1', 't1')
    assert mgr.create_trigger(trg)
    assert mgr.get_trigger_by_name('trg1') == trg
    assert mgr.get_triggers_for_table('t1')[0] == trg

def test_create_duplicate_trigger():
    mgr = TriggerManager()
    trg = make_trigger('trg2', 't2')
    assert mgr.create_trigger(trg)
    assert not mgr.create_trigger(trg)  # duplicate

def test_drop_trigger():
    mgr = TriggerManager()
    trg = make_trigger('trg3', 't3')
    mgr.create_trigger(trg)
    assert mgr.drop_trigger('trg3')
    assert mgr.get_trigger_by_name('trg3') is None
    assert not mgr.drop_trigger('trg3')  # already deleted

def test_get_triggers_for_event():
    mgr = TriggerManager()
    trg = make_trigger('trg4', 't4', timing=TriggerTiming.AFTER, events=[TriggerEvent.UPDATE])
    mgr.create_trigger(trg)
    result = mgr.get_triggers_for_event('t4', TriggerEvent.UPDATE, TriggerTiming.AFTER)
    assert trg in result

def test_execute_triggers_success():
    mgr = TriggerManager()
    trg = make_trigger('trg5', 't5')
    mgr.create_trigger(trg)
    # patch _execute_trigger_body to always return True
    mgr._execute_trigger_body = MagicMock(return_value=True)
    assert mgr.execute_triggers('t5', TriggerEvent.INSERT, TriggerTiming.BEFORE)

def test_execute_triggers_when_condition_false():
    mgr = TriggerManager()
    trg = make_trigger('trg6', 't6')
    mgr.create_trigger(trg)
    # patch when_condition evaluator to always return False
    mgr._condition_evaluator.evaluate_condition = MagicMock(return_value=False)
    mgr._execute_trigger_body = MagicMock(return_value=True)
    assert mgr.execute_triggers('t6', TriggerEvent.INSERT, TriggerTiming.BEFORE)

def test_execute_triggers_body_fail():
    mgr = TriggerManager()
    trg = make_trigger('trg7', 't7')
    mgr.create_trigger(trg)
    mgr._execute_trigger_body = MagicMock(return_value=False)
    assert not mgr.execute_triggers('t7', TriggerEvent.INSERT, TriggerTiming.BEFORE)

def test_get_all_triggers_and_statistics():
    mgr = TriggerManager()
    trg = make_trigger('trg8', 't8')
    mgr.create_trigger(trg)
    assert len(mgr.get_all_triggers()) == 1
    stats = mgr.get_trigger_statistics()
    assert stats['total_triggers'] == 1
    assert stats['triggers_by_timing']['BEFORE'] == 1
    assert stats['triggers_by_event']['INSERT'] == 1

def test_execution_history_and_clear():
    mgr = TriggerManager()
    trg = make_trigger('trg9', 't9')
    mgr.create_trigger(trg)
    mgr._execute_trigger_body = MagicMock(return_value=True)
    mgr.execute_triggers('t9', TriggerEvent.INSERT, TriggerTiming.BEFORE)
    assert len(mgr.get_execution_history()) > 0
    mgr.clear_execution_history()
    assert len(mgr.get_execution_history()) == 0 