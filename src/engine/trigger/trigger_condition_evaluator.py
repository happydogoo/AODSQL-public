# -*- coding: utf-8 -*-
"""
触发器条件评估器 - 负责评估触发器中的WHEN条件和OLD/NEW引用
"""
from typing import Dict, Optional, Any
from src.sql_compiler.ast_nodes import (
    BinaryExpr, Identifier, Literal, OldNewReference, 
    AggregateFunction, Expression
)
import logging

logger = logging.getLogger(__name__)

class TriggerConditionEvaluator:
    """触发器条件评估器"""
    
    def __init__(self):
        self.context = {}
    
    def evaluate_condition(self, condition: Expression, old_data: Optional[Dict[str, Any]], 
                          new_data: Optional[Dict[str, Any]], context: Optional[Dict] = None) -> bool:
        """
        评估触发器条件
        
        Args:
            condition: 条件表达式AST节点
            old_data: 旧数据（UPDATE/DELETE时使用）
            new_data: 新数据（INSERT/UPDATE时使用）
            context: 执行上下文
            
        Returns:
            bool: 条件是否满足
        """
        try:
            # 设置上下文
            self.context = {
                'old_data': old_data or {},
                'new_data': new_data or {},
                'context': context or {}
            }
            
            result = self._evaluate_expression(condition)
            
            # 确保返回布尔值
            if isinstance(result, bool):
                return result
            else:
                # 非布尔值转换为布尔值
                return bool(result)
                
        except Exception as e:
            logger.error(f"评估触发器条件失败: {e}")
            return False
    
    def _evaluate_expression(self, expr: Expression) -> Any:
        """
        递归评估表达式
        
        Args:
            expr: 表达式AST节点
            
        Returns:
            Any: 表达式计算结果
        """
        if isinstance(expr, Literal):
            return self._evaluate_literal(expr)
        elif isinstance(expr, Identifier):
            return self._evaluate_identifier(expr)
        elif isinstance(expr, OldNewReference):
            return self._evaluate_old_new_reference(expr)
        elif isinstance(expr, BinaryExpr):
            return self._evaluate_binary_expr(expr)
        elif isinstance(expr, AggregateFunction):
            return self._evaluate_aggregate_function(expr)
        else:
            logger.warning(f"不支持的表达式类型: {type(expr).__name__}")
            return None
    
    def _evaluate_literal(self, literal: Literal) -> Any:
        """评估字面量"""
        return literal.value
    
    def _evaluate_identifier(self, identifier: Identifier) -> Any:
        """评估标识符（列名）"""
        # 在触发器上下文中，标识符通常指代列名
        # 优先从NEW数据中查找，然后从OLD数据中查找
        column_name = identifier.value
        
        # 从NEW数据中查找
        if 'new_data' in self.context and column_name in self.context['new_data']:
            return self.context['new_data'][column_name]
        
        # 从OLD数据中查找
        if 'old_data' in self.context and column_name in self.context['old_data']:
            return self.context['old_data'][column_name]
        
        # 从上下文中查找
        if 'context' in self.context and column_name in self.context['context']:
            return self.context['context'][column_name]
        
        logger.warning(f"未找到列 {column_name} 的值")
        return None
    
    def _evaluate_old_new_reference(self, ref: OldNewReference) -> Any:
        """评估OLD/NEW引用"""
        column_name = ref.column_name.value if hasattr(ref.column_name, 'value') else str(ref.column_name)
        ref_type = ref.reference_type
        
        if ref_type == 'OLD':
            if 'old_data' in self.context and column_name in self.context['old_data']:
                return self.context['old_data'][column_name]
            else:
                logger.warning(f"OLD.{column_name} 数据不存在")
                return None
        elif ref_type == 'NEW':
            if 'new_data' in self.context and column_name in self.context['new_data']:
                return self.context['new_data'][column_name]
            else:
                logger.warning(f"NEW.{column_name} 数据不存在")
                return None
        else:
            logger.warning(f"不支持的引用类型: {ref_type}")
            return None
    
    def _evaluate_binary_expr(self, binary_expr: BinaryExpr) -> Any:
        """评估二元表达式"""
        left_value = self._evaluate_expression(binary_expr.left)
        right_value = self._evaluate_expression(binary_expr.right)
        operator = binary_expr.operator
        
        # 处理操作符
        if isinstance(operator, tuple):
            op_value = operator[1] if len(operator) > 1 else str(operator)
        else:
            op_value = str(operator)
        
        try:
            if op_value == '=':
                return left_value == right_value
            elif op_value == '!=' or op_value == '<>':
                return left_value != right_value
            elif op_value == '<':
                return left_value < right_value
            elif op_value == '>':
                return left_value > right_value
            elif op_value == '<=':
                return left_value <= right_value
            elif op_value == '>=':
                return left_value >= right_value
            elif op_value == 'AND':
                return bool(left_value) and bool(right_value)
            elif op_value == 'OR':
                return bool(left_value) or bool(right_value)
            elif op_value == 'LIKE':
                return self._evaluate_like(left_value, right_value)
            elif op_value == 'IN':
                return right_value is not None and left_value in right_value
            else:
                logger.warning(f"不支持的二元操作符: {op_value}")
                return False
        except Exception as e:
            logger.error(f"评估二元表达式失败: {e}")
            return False
    
    def _evaluate_like(self, value: Any, pattern: Any) -> bool:
        """评估LIKE操作"""
        if not isinstance(value, str) or not isinstance(pattern, str):
            return False
        
        # 简单的LIKE实现，支持%通配符
        import re
        # 将SQL LIKE模式转换为正则表达式
        regex_pattern = pattern.replace('%', '.*').replace('_', '.')
        return bool(re.match(regex_pattern, value))
    
    def _evaluate_aggregate_function(self, agg_func: AggregateFunction) -> Any:
        """评估聚合函数（在触发器上下文中通常不支持）"""
        logger.warning(f"触发器上下文中不支持聚合函数: {agg_func.function_name}")
        return None
    
    def substitute_old_new_references(self, sql_template: str, old_data: Optional[Dict[str, Any]], 
                                    new_data: Optional[Dict[str, Any]]) -> str:
        """
        替换SQL模板中的OLD/NEW引用
        
        Args:
            sql_template: 包含OLD/NEW引用的SQL模板
            old_data: 旧数据
            new_data: 新数据
            
        Returns:
            str: 替换后的SQL语句
        """
        try:
            result = sql_template
            
            # 替换OLD.column引用
            if old_data:
                for column, value in old_data.items():
                    old_ref = f"OLD.{column}"
                    if isinstance(value, str):
                        result = result.replace(old_ref, f"'{value}'")
                    else:
                        result = result.replace(old_ref, str(value))
            
            # 替换NEW.column引用
            if new_data:
                for column, value in new_data.items():
                    new_ref = f"NEW.{column}"
                    if isinstance(value, str):
                        result = result.replace(new_ref, f"'{value}'")
                    else:
                        result = result.replace(new_ref, str(value))
            
            return result
            
        except Exception as e:
            logger.error(f"替换OLD/NEW引用失败: {e}")
            return sql_template
