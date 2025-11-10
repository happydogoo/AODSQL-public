# -*- coding: utf-8 -*-
"""
执行计划适配器 - 将逻辑执行计划转换为执行器期望的格式
"""
from typing import Dict, Any, List, Optional
from .logical_operators import LogicalOperator, ScanOperator, FilterOperator, ProjectOperator, InsertOperator
from .logical_operators import IndexScanOperator, CreateIndexOperator
from .symbol_table import SymbolTable


class ExecutionPlanAdapter:
    """执行计划适配器"""
    
    def __init__(self, symbol_table: SymbolTable):
        self.symbol_table = symbol_table
    
    def adapt_logical_plan_to_executor_format(self, logical_plan) -> Dict[str, Any]:
        """
        将逻辑执行计划转换为执行器期望的格式
        """
        root_op = logical_plan.root
        
        # 检查操作符类型
        if hasattr(root_op, 'operator_type'):
            op_type = root_op.operator_type.value
        else:
            op_type = str(type(root_op))
        
        # 通过类名检查
        class_name = type(root_op).__name__
        
        if 'Insert' in class_name or isinstance(root_op, InsertOperator):
            return self._adapt_insert_plan(root_op)
        elif 'Project' in class_name or isinstance(root_op, ProjectOperator):
            return self._adapt_select_plan(root_op)
        elif 'IndexScan' in class_name or isinstance(root_op, IndexScanOperator):
            return self._adapt_index_scan_plan(root_op)
        elif 'CreateIndex' in class_name or isinstance(root_op, CreateIndexOperator):
            return self._adapt_create_index_plan(root_op)
        elif 'CreateTrigger' in class_name or op_type == 'CreateTrigger':
            return self._adapt_create_trigger_plan(root_op)
        elif 'DropTrigger' in class_name or op_type == 'DropTrigger':
            return self._adapt_drop_trigger_plan(root_op)
        elif 'ShowTriggers' in class_name or op_type == 'ShowTriggers':
            return self._adapt_show_triggers_plan(root_op)
        else:
            raise ValueError(f"Unsupported logical operator: {type(root_op)}")
    
    def _adapt_insert_plan(self, insert_op) -> Dict[str, Any]:
        """适配INSERT计划"""
        # 安全地获取属性
        table_name = getattr(insert_op, 'table_name', 'unknown')
        values = getattr(insert_op, 'values', [])
        
        # 转换值为简单格式
        simple_values = []
        for value in values:
            if hasattr(value, 'value'):
                simple_values.append(value.value)
            else:
                simple_values.append(str(value))
        
        return {
            "type": "INSERT_PLAN",
            "table_name": table_name,
            "values": simple_values
        }
    
    def _adapt_select_plan(self, project_op) -> Dict[str, Any]:
        """适配SELECT计划"""
        # 从Project操作符中提取信息
        select_list = []
        table_name = None
        where_clause = None
        
        # 安全地获取列信息
        columns = getattr(project_op, 'columns', [])
        for col_ref in columns:
            if hasattr(col_ref, 'column_name'):
                select_list.append(col_ref.column_name)
            else:
                select_list.append(str(col_ref))
        
        # 遍历子操作符找到表名和WHERE条件
        current_op = project_op
        children = getattr(current_op, 'children', [])
        
        while children:
            child = children[0]
            
            # 安全地检查操作符类型
            if hasattr(child, 'operator_type') and child.operator_type.value == 'Scan':
                table_name = getattr(child, 'table_name', 'unknown')
                break
            elif hasattr(child, 'operator_type') and child.operator_type.value == 'Filter':
                condition = getattr(child, 'condition', None)
                where_clause = self._convert_filter_to_executor_format(condition)
                current_op = child
                children = getattr(current_op, 'children', [])
            else:
                current_op = child
                children = getattr(current_op, 'children', [])
        
        return {
            "type": "SELECT_PLAN",
            "table_name": table_name,
            "select_list": select_list,
            "where_clause": where_clause
        }
    
    def _convert_filter_to_executor_format(self, condition) -> Optional[Dict[str, Any]]:
        """将过滤条件转换为执行器格式"""
        if not condition:
            return None
        
        # 简化版：返回字符串表示，执行器可以解析
        return {
            "condition": str(condition),
            "type": "binary_expression"
        }

    def _adapt_index_scan_plan(self, idx_op) -> Dict[str, Any]:
        table_name = getattr(idx_op, 'table_name', 'unknown')
        index_name = getattr(idx_op, 'index_name', 'unknown')
        column_name = getattr(idx_op, 'column_name', 'unknown')
        predicate = getattr(idx_op, 'predicate', None)
        return {
            "type": "INDEX_SCAN_PLAN",
            "table_name": table_name,
            "index_name": index_name,
            "column_name": column_name,
            "predicate": predicate
        }

    def _adapt_create_index_plan(self, create_idx_op) -> Dict[str, Any]:
        # 确保正确获取CreateIndexOperator的属性
        index_name = getattr(create_idx_op, 'index_name', 'unknown')
        table_name = getattr(create_idx_op, 'table_name', 'unknown')
        column_name = getattr(create_idx_op, 'column_name', 'unknown')
        
        # 如果属性值为'unknown'，尝试从其他可能的属性名获取
        if index_name == 'unknown' and hasattr(create_idx_op, 'index'):
            index_name = getattr(create_idx_op.index, 'name', 'unknown') if hasattr(create_idx_op.index, 'name') else 'unknown'
        
        if table_name == 'unknown' and hasattr(create_idx_op, 'table'):
            table_name = getattr(create_idx_op.table, 'name', 'unknown') if hasattr(create_idx_op.table, 'name') else 'unknown'
        
        if column_name == 'unknown' and hasattr(create_idx_op, 'column'):
            column_name = getattr(create_idx_op.column, 'name', 'unknown') if hasattr(create_idx_op.column, 'name') else 'unknown'
        
        return {
            "type": "CREATE_INDEX_PLAN",
            "index_name": index_name,
            "table_name": table_name,
            "column_name": column_name
        }


class SQLCompiler:
    """SQL编译器 - 整合词法、语法、语义分析和执行计划生成"""
    
    def __init__(self, symbol_table: SymbolTable):
        self.symbol_table = symbol_table
        self.adapter = ExecutionPlanAdapter(symbol_table)
    
    def compile(self, sql_text: str) -> Dict[str, Any]:
        """
        编译SQL语句，返回执行器可用的执行计划
        """
        try:
            # 1. 词法分析
            from .lexicalAnalysis import tokenize
            tokens = tokenize(sql_text)
            
            # 2. 语法分析
            from .syntax_adapter import SyntaxAdapter
            syntax_analyzer = SyntaxAdapter(use_new_analyzer=True)
            ast = syntax_analyzer.build_ast_from_tokens(tokens)
            
            # 3. 语义分析
            from .enhanced_semantic_analyzer import EnhancedSemanticAnalyzer as SemanticAnalyzer
            semantic_analyzer = SemanticAnalyzer(self.symbol_table)
            
            if not semantic_analyzer.analyze(ast):
                # 语义分析失败，返回错误信息
                errors = semantic_analyzer.get_errors()
                return {
                    "type": "ERROR",
                    "message": f"语义分析失败: {errors[0].message}" if errors else "未知语义错误"
                }
            
            # 4. 生成逻辑执行计划
            from .enhanced_query_planner import EnhancedQueryPlanner as ExecutionPlanGenerator
            plan_generator = ExecutionPlanGenerator(self.symbol_table)
            logical_plan = plan_generator.planner.create_plan(ast)
            
            # 5. 转换为执行器格式
            executor_plan = self.adapter.adapt_logical_plan_to_executor_format(logical_plan)
            
            return executor_plan
            
        except Exception as e:
            return {
                "type": "ERROR",
                "message": f"编译失败: {str(e)}"
            }
    
    def compile_with_plan_info(self, sql_text: str) -> Dict[str, Any]:
        """
        编译SQL语句，返回包含详细计划信息的字典
        """
        try:
            # 1-3. 词法、语法、语义分析（同上）
            from .lexicalAnalysis import tokenize
            from .syntax_adapter import SyntaxAdapter
            from .enhanced_semantic_analyzer import EnhancedSemanticAnalyzer as SemanticAnalyzer
            
            tokens = tokenize(sql_text)
            syntax_analyzer = SyntaxAdapter(use_new_analyzer=True)
            ast = syntax_analyzer.build_ast_from_tokens(tokens)
            
            semantic_analyzer = SemanticAnalyzer(self.symbol_table)
            if not semantic_analyzer.analyze(ast):
                errors = semantic_analyzer.get_errors()
                return {
                    "type": "ERROR",
                    "message": f"语义分析失败: {errors[0].message}" if errors else "未知语义错误"
                }
            
            # 4. 生成详细执行计划
            from .enhanced_query_planner import EnhancedQueryPlanner as ExecutionPlanGenerator
            plan_generator = ExecutionPlanGenerator(self.symbol_table)
            detailed_plan = plan_generator.generate_plan(ast)
            
            # 5. 转换为执行器格式
            logical_plan = plan_generator.planner.create_plan(ast)
            executor_plan = self.adapter.adapt_logical_plan_to_executor_format(logical_plan)
            
            return {
                "executor_plan": executor_plan,
                "logical_plan": detailed_plan,
                "ast": str(ast),
                "semantic_analysis": {
                    "success": True,
                    "errors": [],
                    "warnings": semantic_analyzer.get_warnings()
                }
            }
            
        except Exception as e:
            return {
                "type": "ERROR",
                "message": f"编译失败: {str(e)}"
            }
    
    def _adapt_create_trigger_plan(self, create_trigger_op) -> Dict[str, Any]:
        """适配CREATE TRIGGER计划"""
        return {
            "type": "CREATE_TRIGGER",
            "trigger_name": getattr(create_trigger_op, 'trigger_name', 'unknown'),
            "table_name": getattr(create_trigger_op, 'table_name', 'unknown'),
            "timing": getattr(create_trigger_op, 'timing', 'unknown'),
            "events": getattr(create_trigger_op, 'events', []),
            "is_row_level": getattr(create_trigger_op, 'is_row_level', False),
            "when_condition": getattr(create_trigger_op, 'when_condition', None),
            "trigger_body": getattr(create_trigger_op, 'trigger_body', [])
        }
    
    def _adapt_drop_trigger_plan(self, drop_trigger_op) -> Dict[str, Any]:
        """适配DROP TRIGGER计划"""
        return {
            "type": "DROP_TRIGGER",
            "trigger_name": getattr(drop_trigger_op, 'trigger_name', 'unknown')
        }
    
    def _adapt_show_triggers_plan(self, show_triggers_op) -> Dict[str, Any]:
        """适配SHOW TRIGGERS计划"""
        return {
            "type": "SHOW_TRIGGERS"
        }