import pytest
from src.engine.trigger.trigger_exception_handler import (
    TriggerExceptionHandler, TriggerException, TriggerSyntaxError, TriggerSemanticError,
    TriggerExecutionError, TriggerConditionError, TriggerErrorType
)

def test_handle_syntax_error():
    handler = TriggerExceptionHandler()
    exc = TriggerSyntaxError("语法错误", "trg1")
    result = handler.handle_exception(exc, "trg1")
    assert result["error"] == "语法错误"
    assert not result["success"]

def test_handle_semantic_error():
    handler = TriggerExceptionHandler()
    exc = TriggerSemanticError("语义错误", "trg2")
    result = handler.handle_exception(exc, "trg2")
    assert result["error"] == "语义错误"
    assert not result["success"]

def test_handle_execution_error():
    handler = TriggerExceptionHandler()
    exc = TriggerExecutionError("执行错误", "trg3")
    result = handler.handle_exception(exc, "trg3")
    assert result["error"] == "执行错误"
    assert not result["success"]

def test_handle_condition_error():
    handler = TriggerExceptionHandler()
    exc = TriggerConditionError("条件错误", "trg4")
    result = handler.handle_exception(exc, "trg4")
    assert result["error"] == "条件错误"
    assert not result["success"]

def test_handle_unknown_error():
    handler = TriggerExceptionHandler()
    exc = Exception("未知错误")
    result = handler.handle_exception(exc, "trg5")
    assert result["error"] == "未知错误"
    assert not result["success"]

def test_error_log_and_clear():
    handler = TriggerExceptionHandler()
    exc = TriggerSyntaxError("语法错误", "trg6")
    handler.handle_exception(exc, "trg6")
    assert len(handler.get_error_log()) > 0
    handler.clear_error_log()
    assert len(handler.get_error_log()) == 0

def test_error_statistics():
    handler = TriggerExceptionHandler()
    handler.clear_error_log()
    handler.handle_exception(TriggerSyntaxError("语法错误", "trg7"), "trg7")
    handler.handle_exception(TriggerSemanticError("语义错误", "trg8"), "trg8")
    stats = handler.get_error_statistics()
    assert stats["total_errors"] == 2
    assert "SYNTAX_ERROR" in stats["errors_by_type"]
    assert "trg7" in stats["errors_by_trigger"]

def test_trigger_exception_str():
    exc = TriggerException("msg", TriggerErrorType.SYNTAX_ERROR, "trg9")
    assert "触发器异常" in str(exc) 