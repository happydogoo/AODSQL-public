import pytest
from src.engine.trigger.trigger_condition_evaluator import TriggerConditionEvaluator
from src.sql_compiler.ast_nodes import Literal, Identifier, BinaryExpr, OldNewReference

class DummyToken:
    def __init__(self, type_, value):
        self.type = type_
        self.literal = value

def make_literal(val):
    return Literal(DummyToken('NUMBER' if isinstance(val, (int, float)) else 'STRING', str(val)), val)

def make_identifier(name):
    return Identifier(DummyToken('IDENTIFIER', name), name)

def test_literal_evaluation():
    evaluator = TriggerConditionEvaluator()
    assert evaluator._evaluate_literal(make_literal(123)) == 123
    assert evaluator._evaluate_literal(make_literal("abc")) == "abc"

def test_identifier_evaluation():
    evaluator = TriggerConditionEvaluator()
    evaluator.context = {'new_data': {'col1': 42}, 'old_data': {}, 'context': {}}
    id1 = make_identifier('col1')
    assert evaluator._evaluate_identifier(id1) == 42
    evaluator.context = {'new_data': {}, 'old_data': {'col2': 'x'}, 'context': {}}
    id2 = make_identifier('col2')
    assert evaluator._evaluate_identifier(id2) == 'x'
    evaluator.context = {'new_data': {}, 'old_data': {}, 'context': {'col3': 99}}
    id3 = make_identifier('col3')
    assert evaluator._evaluate_identifier(id3) == 99

def test_old_new_reference():
    evaluator = TriggerConditionEvaluator()
    ref_old = OldNewReference('OLD', make_identifier('foo'))
    ref_new = OldNewReference('NEW', make_identifier('bar'))
    evaluator.context = {'old_data': {'foo': 1}, 'new_data': {'bar': 2}, 'context': {}}
    assert evaluator._evaluate_old_new_reference(ref_old) == 1
    assert evaluator._evaluate_old_new_reference(ref_new) == 2

def test_binary_expr():
    evaluator = TriggerConditionEvaluator()
    expr = BinaryExpr(make_literal(1), ('=', '='), make_literal(1))
    assert evaluator._evaluate_binary_expr(expr) is True
    expr = BinaryExpr(make_literal(2), ('>', '>'), make_literal(1))
    assert evaluator._evaluate_binary_expr(expr) is True
    expr = BinaryExpr(make_literal(1), ('<', '<'), make_literal(2))
    assert evaluator._evaluate_binary_expr(expr) is True
    expr = BinaryExpr(make_literal(1), ('!=', '!='), make_literal(2))
    assert evaluator._evaluate_binary_expr(expr) is True
    expr = BinaryExpr(make_literal(1), ('AND', 'AND'), make_literal(0))
    assert evaluator._evaluate_binary_expr(expr) is False

def test_evaluate_condition_bool_and_nonbool():
    evaluator = TriggerConditionEvaluator()
    cond = BinaryExpr(make_literal(1), ('=', '='), make_literal(1))
    assert evaluator.evaluate_condition(cond, {}, {}) is True
    cond = make_literal(0)
    assert evaluator.evaluate_condition(cond, {}, {}) is False

def test_like_and_in():
    evaluator = TriggerConditionEvaluator()
    expr = BinaryExpr(make_literal('abc123'), ('LIKE', 'LIKE'), make_literal('abc%'))
    assert evaluator._evaluate_binary_expr(expr) is True
    expr = BinaryExpr(make_literal(2), ('IN', 'IN'), make_literal([1,2,3]))
    assert evaluator._evaluate_binary_expr(expr) is True

def test_unsupported_expr_type():
    evaluator = TriggerConditionEvaluator()
    class DummyExpr: pass
    assert evaluator._evaluate_expression(DummyExpr()) is None 