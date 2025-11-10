# -*- coding: utf-8 -*-
"""
增强的查询规划器 - 支持JOIN、子查询、聚合函数
"""
from typing import List, Optional, Dict, Any, Union
from .logical_operators import (
    LogicalOperator, ScanOperator, FilterOperator, ProjectOperator,
    InsertOperator, UpdateOperator, DeleteOperator, LogicalPlan,
    CreateViewOperator, DropViewOperator, AlterViewOperator,
    CreateTableOperator, DropTableOperator, CreateTriggerOperator, DropTriggerOperator,
    CreateIndexOperator, DropIndexOperator,
    ShowTablesOperator, ShowColumnsOperator, ShowIndexOperator, ShowTriggersOperator, ShowViewsOperator,
    ExplainOperator, JoinOperator, SortOperator, LimitOperator, AggregateOperator,
    GroupByOperator, HavingOperator, DeclareCursorOperator, OpenCursorOperator,
    FetchCursorOperator, CloseCursorOperator,
    BeginTransactionOperator, CommitTransactionOperator, RollbackTransactionOperator,
    ColumnReference, Expression, LiteralExpression, ColumnExpression, BinaryExpression
)
from .symbol_table import SymbolTable, DataType
import sys
import os

# 添加路径以便导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class EnhancedQueryPlanner:
    """增强的查询规划器"""
    
    def __init__(self, symbol_table: SymbolTable):
        self.symbol_table = symbol_table
    
    def create_plan(self, ast_node) -> LogicalPlan:
        """从AST创建逻辑执行计划"""
        if hasattr(ast_node, 'query'):
            query_node = ast_node.query
        else:
            query_node = ast_node
        
        # 检查不同类型的查询 - 按优先级排序
        # 0. 事务语句 - 检查类型
        if hasattr(query_node, 'type'):
            if query_node.type == 'BeginTransactionStatement':
                return self._create_begin_transaction_plan(query_node)
            elif query_node.type == 'CommitTransactionStatement':
                return self._create_commit_transaction_plan(query_node)
            elif query_node.type == 'RollbackTransactionStatement':
                return self._create_rollback_transaction_plan(query_node)
        
        # 1. CREATE TABLE - 有table和columns，但没有values和where
        if (hasattr(query_node, 'table') and hasattr(query_node, 'columns') and 
            not hasattr(query_node, 'values') and not hasattr(query_node, 'where')):
            return self._create_create_table_plan(query_node)
        # 2. INSERT - 有values
        elif hasattr(query_node, 'values'):
            return self._create_insert_plan(query_node)
        # 3. UPDATE - 有set_list
        elif hasattr(query_node, 'set_list'):
            return self._create_update_plan(query_node)
        # 4. DELETE - 有table和where，但没有columns
        elif (hasattr(query_node, 'table') and hasattr(query_node, 'where') and 
              not hasattr(query_node, 'columns')):
            return self._create_delete_plan(query_node)
        # 5. DROP INDEX - 有index_name属性且类名包含DropIndex
        elif (hasattr(query_node, 'index_name') and 
              'DropIndex' in str(type(query_node))):
            return self._create_drop_index_plan(query_node)
        # 6. DROP TABLE - 有table但没有columns、values、where
        elif (hasattr(query_node, 'table') and not hasattr(query_node, 'columns') and 
              not hasattr(query_node, 'values') and not hasattr(query_node, 'where')):
            return self._create_drop_table_plan(query_node)

        # 6. ALTER VIEW - 有view_name和select_statement（通过类型判断）- 更具体的检查优先
        elif hasattr(query_node, 'view_name') and hasattr(query_node, 'select_statement') and hasattr(query_node, '__class__') and 'Alter' in query_node.__class__.__name__:
            return self._create_alter_view_plan(query_node)
        # 7. CREATE VIEW - 有view_name和select_statement
        elif hasattr(query_node, 'view_name') and hasattr(query_node, 'select_statement'):
            return self._create_create_view_plan(query_node)
        # 8. DROP VIEW - 有view_name但没有select_statement
        elif hasattr(query_node, 'view_name') and not hasattr(query_node, 'select_statement'):
            return self._create_drop_view_plan(query_node)
        # 6. CREATE TRIGGER - 有trigger_name属性且类名包含CreateTrigger
        elif (hasattr(query_node, 'trigger_name') and 
              'CreateTrigger' in str(type(query_node))):
            return self._create_create_trigger_plan(query_node)
        # 7. DROP TRIGGER - 有trigger_name属性且类名包含DropTrigger
        elif (hasattr(query_node, 'trigger_name') and 
              'DropTrigger' in str(type(query_node))):
            return self._create_drop_trigger_plan(query_node)
        # 8. SHOW TRIGGERS - 类名包含ShowTriggers
        elif 'ShowTriggers' in str(type(query_node)):
            return self._create_show_triggers_plan(query_node)
        # 9. SHOW VIEWS - 类名包含ShowViews
        elif 'ShowViews' in str(type(query_node)):
            return self._create_show_plan(query_node)
        # 10. DECLARE CURSOR - 类名包含DeclareCursor
        elif 'DeclareCursor' in str(type(query_node)):
            return self._create_declare_cursor_plan(query_node)
        # 10. OPEN CURSOR - 类名包含OpenCursor
        elif 'OpenCursor' in str(type(query_node)):
            return self._create_open_cursor_plan(query_node)
        # 11. FETCH CURSOR - 类名包含FetchCursor
        elif 'FetchCursor' in str(type(query_node)):
            return self._create_fetch_cursor_plan(query_node)
        # 12. CLOSE CURSOR - 类名包含CloseCursor
        elif 'CloseCursor' in str(type(query_node)):
            return self._create_close_cursor_plan(query_node)
        # 9. EXPLAIN - 有query属性
        elif hasattr(query_node, 'query'):
            return self._create_explain_plan(query_node)
        # 10. SHOW COLUMNS/INDEX - 有table_name属性但没有columns属性
        elif hasattr(query_node, 'table_name') and not hasattr(query_node, 'columns'):
            return self._create_show_plan(query_node)
        # 11. SHOW TABLES - 没有table属性和table_name属性
        elif not hasattr(query_node, 'table') and not hasattr(query_node, 'columns') and not hasattr(query_node, 'table_name'):
            return self._create_show_tables_plan(query_node)

        # 12. CREATE INDEX - 有index_name属性
        elif hasattr(query_node, 'index_name'):
            return self._create_create_index_plan(query_node)
        # 13. SELECT - 有columns和table/table_name，可能有where
        elif hasattr(query_node, 'columns') and (hasattr(query_node, 'table') or hasattr(query_node, 'table_name')):

            return self._create_select_plan(query_node)
        else:
            raise ValueError(f"Unsupported query type: {type(query_node)}")
    
    def _create_select_plan(self, select_node) -> LogicalPlan:
        """创建SELECT查询的执行计划"""
        current_op = None
        table_name = "unknown"  # 初始化一个默认表名

        # 1. 确定 FROM 子句的源节点
        source_table_node = getattr(select_node, 'table', None) or \
                            getattr(select_node, 'table_name', None) or \
                            (select_node.tables[0] if hasattr(select_node, 'tables') and select_node.tables else None)

        if not source_table_node:
            raise ValueError("SELECT statement must have a valid FROM clause.")


        # 检查是否是多表查询
        if hasattr(source_table_node, 'tables'):
            # --- 情况一：多表查询（笛卡尔积） ---
            tables = source_table_node.tables
            if len(tables) < 2:
                raise ValueError("多表查询至少需要两个表")
            
            # 创建第一个表的扫描
            first_table = tables[0]
            table_name = first_table.table_name
            scan_op = ScanOperator(table_name)
            current_op = scan_op
            
            # 逐步创建笛卡尔积
            for i in range(1, len(tables)):
                table = tables[i]
                table_name = table.table_name
                right_scan = ScanOperator(table_name)
                
                # 创建笛卡尔积（无条件连接）
                from .logical_operators import JoinType, JoinMethod
                join_op = JoinOperator(JoinType.CARTESIAN, None, JoinMethod.NESTED_LOOP)
                join_op.left_child = current_op
                join_op.right_child = right_scan
                # 确保children属性被正确设置
                join_op.children = [current_op, right_scan]
                join_op._children_set = True
                current_op = join_op

        elif hasattr(source_table_node, 'subquery') or "Subquery" in str(type(source_table_node)):
            # --- 情况二：FROM 子句是子查询 ---
            subquery_ast = getattr(source_table_node, 'subquery', source_table_node)
            subquery_plan = self._create_select_plan(subquery_ast)
            current_op = subquery_plan.root  # 假设 LogicalPlan 有 root 属性
            table_name = "subquery_result"

        else:
            # --- 情况三：FROM 子句是普通表 ---
            
            # 【修复点 1】: 稳健地提取表名，而不是使用对象的字符串表示
            if hasattr(source_table_node, 'name'):
                table_name = source_table_node.name
            elif hasattr(source_table_node, 'table_name'):
                table_name = source_table_node.table_name
            else:
                # 从 'TableReference(customers)' 这种字符串中提取 'customers'
                raw_str = str(source_table_node)
                import re
                match = re.search(r'\((\w+)\)', raw_str)
                if match:
                    table_name = match.group(1)
                else:
                    table_name = raw_str

            scan_op = ScanOperator(table_name)
            current_op = scan_op
        
        # 2. 处理JOIN操作
        if hasattr(select_node, 'joins') and select_node.joins:
            for join in select_node.joins:
                # 获取右表名
                right_table_name = join.right_table.table_name if hasattr(join.right_table, 'table_name') else str(join.right_table)
                join_scan = ScanOperator(right_table_name)
                
                # 创建JOIN操作符
                join_op = self._create_join_operator(current_op, join_scan, join.condition)
                current_op = join_op
        
        # 3. 创建过滤操作符（WHERE子句）
        where_clause = getattr(select_node, 'where', getattr(select_node, 'where_clause', None))

        # 【修复点 2】: 增加对 where_clause 类型的防御性检查
        if where_clause and isinstance(where_clause, str):
            where_clause = None  # 将其置空以避免崩溃

        if where_clause:
            filter_condition = self._convert_expression(where_clause, table_name)  # table_name 现在可以是 "subquery_result"
            filter_op = FilterOperator(filter_condition)
            filter_op.add_child(current_op)
            current_op = filter_op
        
        # 4. 检查是否有聚合函数
        columns = self._extract_columns(select_node.columns, table_name)
        
        # 【修复】: 更准确的聚合函数检测
        has_aggregate = False
        for col in columns:
            if isinstance(col, str):
                # 检查字符串形式的聚合函数
                if 'AggregateFunction(' in col or 'COUNT(' in col or 'SUM(' in col or 'AVG(' in col or 'MAX(' in col or 'MIN(' in col:
                    has_aggregate = True
                    break
            elif hasattr(col, 'func_name') or hasattr(col, 'function_name'):
                has_aggregate = True
                break
        
        if has_aggregate:
            # 有聚合函数，创建聚合操作符（包含GROUP BY和HAVING逻辑）
            group_by_columns = getattr(select_node, 'group_by', None)
            having_clause = getattr(select_node, 'having_clause', None)
            current_op = self._create_aggregate_operator(columns, table_name, current_op, group_by_columns, having_clause)
        else:
            # 没有聚合函数，创建投影操作符
            project_op = ProjectOperator(columns)
            project_op.add_child(current_op)
            current_op = project_op
        
        # 7. 处理ORDER BY
        if hasattr(select_node, 'order_by') and select_node.order_by:
            order_op = self._create_order_by_operator(select_node.order_by, table_name)
            order_op.add_child(current_op)
            current_op = order_op
        
        # 8. 处理LIMIT
        if hasattr(select_node, 'limit') and select_node.limit:
            limit_op = self._create_limit_operator(select_node.limit)
            limit_op.add_child(current_op)
            current_op = limit_op
        
        return LogicalPlan(current_op)
    
    def _create_join_operator(self, left_op, right_op, condition):
        """创建JOIN操作符"""
        from .logical_operators import JoinType, JoinMethod
        join_op = JoinOperator(JoinType.INNER, condition, JoinMethod.NESTED_LOOP)
        join_op.add_child(left_op)
        join_op.add_child(right_op)
        return join_op
    
    def _create_group_by_operator(self, group_by_columns, table_name):
        """创建GROUP BY操作符"""
        # 提取GROUP BY列名
        if hasattr(group_by_columns, 'columns'):
            columns = group_by_columns.columns
        else:
            columns = [group_by_columns]
        
        return GroupByOperator(columns)
    
    def _create_having_operator(self, having_node):
        """创建HAVING操作符"""
        return HavingOperator(having_node)
    
    def _create_order_by_operator(self, order_by_items, table_name):
        """创建ORDER BY操作符"""
        from .logical_operators import SortOperator
        
        # 转换order_by_items为正确的格式
        items = []
        
        # 检查是否是OrderByList（多列排序）
        if hasattr(order_by_items, 'order_columns'):
            # 这是OrderByList，处理多个排序列
            for order_col in order_by_items.order_columns:
                if hasattr(order_col, 'column') and hasattr(order_col, 'direction'):
                    # 提取列名
                    if hasattr(order_col.column, 'name'):
                        column_name = order_col.column.name
                    elif hasattr(order_col.column, 'value'):
                        column_name = order_col.column.value
                    else:
                        column_name = str(order_col.column)
                    items.append({"column": column_name, "direction": order_col.direction})
        # 检查是否是单个ORDER BY项目（不是列表）
        elif hasattr(order_by_items, 'column') and hasattr(order_by_items, 'direction'):
            # 提取列名
            if hasattr(order_by_items.column, 'name'):
                column_name = order_by_items.column.name
            elif hasattr(order_by_items.column, 'value'):
                column_name = order_by_items.column.value
            else:
                column_name = str(order_by_items.column)
            items.append({"column": column_name, "direction": order_by_items.direction})
        elif hasattr(order_by_items, '__iter__'):
            for item in order_by_items:
                if hasattr(item, 'column') and hasattr(item, 'direction'):
                    items.append({"column": item.column, "direction": item.direction})
                elif isinstance(item, dict):
                    items.append(item)
                else:
                    # 如果item是字符串，假设是列名，默认为ASC
                    items.append({"column": str(item), "direction": "ASC"})
        
        # 拆分为列名和排序方向
        order_by = []
        ascending = []
        for item in items:
            if isinstance(item, dict):
                order_by.append(item["column"])
                ascending.append(item["direction"] == "ASC")
            else:
                # 如果是字符串，默认为ASC
                order_by.append(str(item))
                ascending.append(True)
        
        return SortOperator(order_by, ascending)
    
    def _create_limit_operator(self, limit):
        """创建LIMIT操作符"""
        return LimitOperator(limit.limit, limit.offset)
    
    def _extract_columns(self, columns_node, table_name):
        """
        稳健地从各种AST节点类型中提取列名字符串。
        """
        # 处理 SELECT * 的情况
        if columns_node == '*' or (isinstance(columns_node, list) and columns_node and str(columns_node[0]) == '*'):
            # 在实际应用中，这里应该查询元数据来获取所有列。
            # 为了简化，我们只返回一个通配符，由后续步骤处理。
            return [f"{table_name}.*"]

        final_columns = []
        
        # 确保我们处理的是一个列表
        items_to_process = columns_node
        if not isinstance(items_to_process, list):
            items_to_process = [items_to_process]

        for item in items_to_process:
            col_name = None
            
            # 【最终修复】: 智能地从AST节点提取列名
            # 优先级 1: 节点有 'value' 属性 (通常是 Identifier 对象)
            if hasattr(item, 'value'):
                col_name = item.value
            # 优先级 2: 节点有 'name' 属性
            elif hasattr(item, 'name'):
                col_name = item.name
            # 优先级 3: 节点本身就是字符串
            elif isinstance(item, str):
                col_name = item
            # 最后的回退策略: 将对象转换为字符串
            else:
                col_name = str(item)

            # 应用上一轮修复的逻辑：如果列名已包含'.'，则不加前缀
            if '.' in col_name:
                final_columns.append(col_name)
            else:
                # 为无别名的列（如外层查询的 'name'）添加来源前缀（'subquery_result'）
                final_columns.append(f"{table_name}.{col_name}")

        return final_columns
    
    def _convert_aggregate_function(self, agg_func, table_name):
        """转换聚合函数"""
        from .logical_operators import AggregateExpression
        
        if hasattr(agg_func, 'arg') and hasattr(agg_func.arg, 'column'):
            column_ref = ColumnReference(table_name, agg_func.arg.column) if agg_func.arg.column != '*' else None
            return AggregateExpression(agg_func.func_name, column_ref, agg_func.arg.distinct)
        else:
            return AggregateExpression(agg_func.func_name, None, False)
    
    def _convert_new_aggregate_function(self, agg_func, table_name):
        """转换新的聚合函数格式"""
        from .logical_operators import AggregateExpression
        
        if agg_func.argument:
            # 处理不同类型的参数
            if hasattr(agg_func.argument, 'value'):
                # Identifier 或 Literal
                column_ref = ColumnReference(table_name, agg_func.argument.value)
            else:
                # BinaryExpr 或其他复杂表达式
                # 对于复杂表达式，我们暂时使用字符串表示
                column_ref = ColumnReference(table_name, str(agg_func.argument))
        else:
            column_ref = None
        
        # 处理别名
        alias = getattr(agg_func, 'alias', None)
        
        return AggregateExpression(agg_func.function_name, column_ref, agg_func.distinct, alias)
    
    def _create_aggregate_operator(self, columns, table_name, child_op, group_by_columns=None, having_clause=None):
        """创建聚合操作符 (最终修复版)"""
        # --- 确保 group_by_columns 是一个节点列表 ---
        group_by_list = []
        if group_by_columns:
            if hasattr(group_by_columns, 'columns'):
                group_by_list = group_by_columns.columns
            else:
                group_by_list = group_by_columns if isinstance(group_by_columns, list) else [group_by_columns]

        return AggregateOperator(columns, table_name, child_op, group_by_list, having_clause)
    
    def _convert_subquery(self, subquery, table_name):
        """转换子查询"""
        from .logical_operators import SubqueryExpression
        
        # 递归创建子查询的执行计划
        subquery_plan = self._create_select_plan(subquery)
        return SubqueryExpression(subquery_plan)
    
    def _convert_expression(self, expr_node, table_name: str) -> Expression:
        """将AST表达式转换为逻辑表达式"""
        # 【防御性检查】: 如果表达式是字符串，返回一个占位符表达式
        if isinstance(expr_node, str):
            return LiteralExpression(expr_node)
        
        if hasattr(expr_node, 'left') and hasattr(expr_node, 'right'):
            # 二元表达式
            left_expr = self._convert_expression(expr_node.left, table_name)
            right_expr = self._convert_expression(expr_node.right, table_name)
            # 获取操作符
            if hasattr(expr_node, 'op'):
                op = expr_node.op
            elif hasattr(expr_node, 'operator'):
                op = expr_node.operator
            else:
                op = '='
            return BinaryExpression(left_expr, op, right_expr)
        elif hasattr(expr_node, 'name'):
            # 标识符
            return ColumnExpression(ColumnReference(table_name, expr_node.name))
        elif hasattr(expr_node, 'value'):
            # 字面量
            return self._convert_literal(expr_node)
        elif hasattr(expr_node, 'func_name'):
            # 聚合函数
            return self._convert_aggregate_function(expr_node, table_name)
        elif hasattr(expr_node, 'columns'):
            # 子查询
            return self._convert_subquery(expr_node, table_name)
        elif hasattr(expr_node, 'subquery'):
            # IN子查询表达式
            return self._convert_in_subquery_expression(expr_node, table_name)
        elif hasattr(expr_node, 'values'):
            # IN值列表表达式
            return self._convert_in_expression(expr_node, table_name)
        elif hasattr(expr_node, 'start') and hasattr(expr_node, 'end'):
            # BETWEEN条件表达式
            return self._convert_between_expression(expr_node, table_name)
        elif hasattr(expr_node, 'type') and hasattr(expr_node, 'value'):
            # Token对象
            if expr_node.type == 'IDENTIFIER':
                return ColumnExpression(ColumnReference(table_name, expr_node.value))
            else:
                return LiteralExpression(expr_node.value)
        else:
            raise ValueError(f"Unknown expression type: {type(expr_node)}")
    
    def _convert_between_expression(self, expr_node, table_name: str) -> Expression:
        """转换BETWEEN表达式"""
        # BETWEEN条件转换为两个比较条件的AND组合
        # column BETWEEN start AND end => column >= start AND column <= end
        
        left_expr = self._convert_expression(expr_node.left, table_name)
        start_expr = self._convert_expression(expr_node.start, table_name)
        end_expr = self._convert_expression(expr_node.end, table_name)
        
        # 创建两个比较表达式
        if hasattr(expr_node, 'negated') and expr_node.negated:
            # NOT BETWEEN => column < start OR column > end
            left_cond = BinaryExpression(left_expr, '<', start_expr)
            right_cond = BinaryExpression(left_expr, '>', end_expr)
            return BinaryExpression(left_cond, 'OR', right_cond)
        else:
            # BETWEEN => column >= start AND column <= end
            left_cond = BinaryExpression(left_expr, '>=', start_expr)
            right_cond = BinaryExpression(left_expr, '<=', end_expr)
            return BinaryExpression(left_cond, 'AND', right_cond)
    
    def _convert_literal(self, literal_node) -> LiteralExpression:
        """转换字面量节点"""
        return LiteralExpression(literal_node.value)
    
    def _convert_in_subquery_expression(self, expr_node, table_name: str) -> Expression:
        """转换IN子查询表达式"""
        from .logical_operators import InSubqueryExpression
        
        # 转换左操作数
        left_expr = self._convert_expression(expr_node.left, table_name)
        
        # 转换子查询
        subquery_expr = self._convert_subquery(expr_node.subquery, table_name)
        
        # 创建IN子查询表达式
        return InSubqueryExpression(left_expr, subquery_expr, expr_node.operator)
    
    def _convert_in_expression(self, expr_node, table_name: str) -> Expression:
        """转换IN值列表表达式"""
        from .logical_operators import InExpression
        
        # 转换左操作数
        left_expr = self._convert_expression(expr_node.left, table_name)
        
        # 转换值列表
        values = []
        for value in expr_node.values:
            if hasattr(value, 'value'):
                values.append(LiteralExpression(value.value))
            else:
                values.append(LiteralExpression(str(value)))
        
        # 创建IN表达式
        return InExpression(left_expr, values, expr_node.operator)
    
    def _create_insert_plan(self, insert_node) -> LogicalPlan:
        """创建INSERT语句的执行计划"""
        table_name = insert_node.table.name
        
        # 处理多行INSERT
        if isinstance(insert_node.values, list) and len(insert_node.values) > 0 and isinstance(insert_node.values[0], list):
            # 多行VALUES格式：[[row1], [row2], ...]
            values = [[self._convert_literal(value) for value in row] for row in insert_node.values]
        else:
            # 单行VALUES格式：[val1, val2, ...]
            values = [self._convert_literal(value) for value in insert_node.values]
        
        insert_op = InsertOperator(table_name, values)
        return LogicalPlan(insert_op)
    
    def _create_update_plan(self, update_node) -> LogicalPlan:
        """创建UPDATE语句的执行计划"""
        table_name = update_node.table.name
        where_clause = None
        
        if update_node.where:
            where_clause = self._convert_expression(update_node.where, table_name)
        
        update_op = UpdateOperator(table_name, update_node.set_list, where_clause)
        return LogicalPlan(update_op)
    
    def _create_delete_plan(self, delete_node) -> LogicalPlan:
        """创建DELETE语句的执行计划"""
        table_name = delete_node.table.name
        where_clause = None
        
        if delete_node.where:
            where_clause = self._convert_expression(delete_node.where, table_name)
        
        delete_op = DeleteOperator(table_name, where_clause)
        return LogicalPlan(delete_op)
    
    def _create_create_table_plan(self, create_table_node) -> LogicalPlan:
        """创建CREATE TABLE语句的执行计划"""
        table_name = create_table_node.table.name
        columns = create_table_node.columns
        
        create_table_op = CreateTableOperator(table_name, columns)
        return LogicalPlan(create_table_op)
    
    def _create_drop_table_plan(self, drop_table_node) -> LogicalPlan:
        """创建DROP TABLE语句的执行计划"""
        table_name = drop_table_node.table.name
        if_exists = getattr(drop_table_node, 'if_exists', False)
        
        drop_table_op = DropTableOperator(table_name, if_exists)
        return LogicalPlan(drop_table_op)
    
    def _create_drop_index_plan(self, drop_index_node) -> LogicalPlan:
        """创建DROP INDEX语句的执行计划"""
        index_name = drop_index_node.index_name
        
        drop_index_op = DropIndexOperator(index_name)
        
        return LogicalPlan(drop_index_op)
    
    def _create_show_tables_plan(self, show_tables_node) -> LogicalPlan:
        """创建SHOW TABLES语句的执行计划"""
        show_tables_op = ShowTablesOperator()
        return LogicalPlan(show_tables_op)
    
    def _create_show_plan(self, show_node) -> LogicalPlan:
        """创建SHOW COLUMNS/INDEX/VIEWS语句的执行计划"""
        # 根据节点类型确定show_type
        node_type_name = type(show_node).__name__.lower()
        
        if hasattr(show_node, 'table_name'):
            if 'columns' in node_type_name:
                show_op = ShowColumnsOperator(show_node.table_name)
            elif 'index' in node_type_name:
                show_op = ShowIndexOperator(show_node.table_name)
            else:
                show_op = ShowTablesOperator()
        elif 'views' in node_type_name:
            show_op = ShowViewsOperator()
        else:
            show_op = ShowTablesOperator()
        
        return LogicalPlan(show_op)
    
    def _create_explain_plan(self, explain_node) -> LogicalPlan:
        """创建EXPLAIN语句的执行计划"""
        explain_op = ExplainOperator(explain_node.query)
        return LogicalPlan(explain_op)
    
    def _create_create_index_plan(self, create_index_node) -> LogicalPlan:
        """创建CREATE INDEX语句的执行计划"""
        # 获取表名
        table_name = create_index_node.table_name if hasattr(create_index_node, 'table_name') else (create_index_node.table.name if hasattr(create_index_node.table, 'name') else str(create_index_node.table))
        
        # 获取列信息（支持多列）
        if hasattr(create_index_node, 'columns'):
            # 多列索引
            columns = create_index_node.columns
        elif hasattr(create_index_node, 'column_name'):
            # 单列索引（向后兼容）
            columns = create_index_node.column_name
        else:
            # 从column属性获取
            columns = create_index_node.column.name if hasattr(create_index_node.column, 'name') else str(create_index_node.column)
        
        create_index_op = CreateIndexOperator(
            create_index_node.index_name,
            table_name,
            columns
        )
        return LogicalPlan(create_index_op)
    
    def _create_join_operator(self, left_child, right_child, condition):
        """创建JOIN操作符"""
        # 解析连接条件，获取连接键的列索引
        left_key_indices = self._get_join_key_indices(condition.left_table, condition.left_column)
        right_key_indices = self._get_join_key_indices(condition.right_table, condition.right_column)
        
        # 创建JOIN操作符
        from .logical_operators import JoinType, JoinMethod
        join_op = JoinOperator(JoinType.INNER, condition, JoinMethod.NESTED_LOOP)
        join_op.add_child(left_child)
        join_op.add_child(right_child)
        return join_op
    
    def _get_join_key_indices(self, table_name, column_name):
        """获取连接键的列索引"""
        # 简化实现：假设连接键是第一列
        # 在实际实现中，应该根据表结构查找列索引
        return [0]
    
    def _create_create_view_plan(self, create_view_node):
        """创建CREATE VIEW计划"""
        # 将SELECT语句转换为字符串
        definition = self._select_to_string(create_view_node.select_statement)
        
        create_view_op = CreateViewOperator(
            view_name=create_view_node.view_name,
            definition=definition,
            schema_name='public',  # 默认模式
            creator='system',      # 默认创建者
            is_updatable=False     # 默认不可更新
        )
        return LogicalPlan(create_view_op)
    
    def _create_drop_view_plan(self, drop_view_node):
        """创建DROP VIEW计划"""
        drop_view_op = DropViewOperator(
            view_name=drop_view_node.view_name
        )
        return LogicalPlan(drop_view_op)
    
    def _create_alter_view_plan(self, alter_view_node):
        """创建ALTER VIEW计划"""
        # 将SELECT语句转换为字符串
        definition = self._select_to_string(alter_view_node.select_statement)
        
        alter_view_op = AlterViewOperator(
            view_name=alter_view_node.view_name,
            definition=definition,
            is_updatable=None  # 保持原有设置
        )
        return LogicalPlan(alter_view_op)
    
    def _select_to_string(self, select_node):
        """将SELECT节点转换为SQL字符串"""
        # 简化实现：返回基本的SELECT语句
        columns = select_node.columns
        table = select_node.table_name
        
        if isinstance(columns, list) and len(columns) == 1 and columns[0] == '*':
            columns_str = '*'
        else:
            # 处理列，包括聚合函数
            column_strings = []
            for col in columns:
                if hasattr(col, 'function_name'):  # 聚合函数
                    func_name = col.function_name
                    argument = col.argument.value if hasattr(col.argument, 'value') else str(col.argument)
                    distinct_str = 'DISTINCT ' if col.distinct else ''
                    alias_str = f" AS {col.alias}" if col.alias else ''
                    column_strings.append(f"{func_name}({distinct_str}{argument}){alias_str}")
                else:
                    column_strings.append(str(col))
            columns_str = ', '.join(column_strings)
        
        sql = f"SELECT {columns_str} FROM {table}"
        
        # 添加JOIN子句
        if hasattr(select_node, 'joins') and select_node.joins:
            for join in select_node.joins:
                if hasattr(join, 'join_type') and hasattr(join, 'right_table') and hasattr(join, 'condition'):
                    join_type = join.join_type
                    right_table = join.right_table
                    condition = join.condition
                    
                    # 构建RIGHT TABLE字符串
                    if hasattr(right_table, 'table_name') and hasattr(right_table, 'alias'):
                        if right_table.alias:
                            right_table_str = f"{right_table.table_name} {right_table.alias}"
                        else:
                            right_table_str = right_table.table_name
                    else:
                        right_table_str = str(right_table)
                    
                    # 构建JOIN条件字符串
                    if hasattr(condition, 'left_table') and hasattr(condition, 'left_column') and \
                       hasattr(condition, 'operator') and hasattr(condition, 'right_table') and \
                       hasattr(condition, 'right_column'):
                        condition_str = f"{condition.left_table}.{condition.left_column} {condition.operator} {condition.right_table}.{condition.right_column}"
                    else:
                        condition_str = str(condition)
                    
                    sql += f" {join_type} JOIN {right_table_str} ON {condition_str}"
        
        # 添加WHERE子句
        if hasattr(select_node, 'where_clause') and select_node.where_clause:
            sql += f" WHERE {select_node.where_clause}"
        
        # 添加GROUP BY子句
        if hasattr(select_node, 'group_by') and select_node.group_by:
            if hasattr(select_node.group_by, 'columns'):
                group_columns = ', '.join(select_node.group_by.columns)
                sql += f" GROUP BY {group_columns}"
        
        # 添加ORDER BY子句
        if hasattr(select_node, 'order_by') and select_node.order_by:
            if hasattr(select_node.order_by, 'column'):
                sql += f" ORDER BY {select_node.order_by.column} {select_node.order_by.direction}"
            else:
                # 处理OrderByList
                order_items = []
                for order_item in select_node.order_by.order_columns:
                    order_items.append(f"{order_item.column} {order_item.direction}")
                sql += f" ORDER BY {', '.join(order_items)}"
        
        return sql

    def _create_create_trigger_plan(self, create_trigger_node) -> LogicalPlan:
        """创建CREATE TRIGGER语句的执行计划"""
        trigger_name = create_trigger_node.trigger_name.value
        table_name = create_trigger_node.table_name.value
        timing = create_trigger_node.timing
        events = create_trigger_node.events
        is_row_level = create_trigger_node.is_row_level
        when_condition = create_trigger_node.when_condition
        trigger_body = create_trigger_node.trigger_body
        
        create_trigger_op = CreateTriggerOperator(
            trigger_name, table_name, timing, events, 
            is_row_level, when_condition, trigger_body
        )
        return LogicalPlan(create_trigger_op)
    
    def _create_drop_trigger_plan(self, drop_trigger_node) -> LogicalPlan:
        """创建DROP TRIGGER语句的执行计划"""
        trigger_name = drop_trigger_node.trigger_name.value
        
        drop_trigger_op = DropTriggerOperator(trigger_name)
        return LogicalPlan(drop_trigger_op)
    
    def _create_show_triggers_plan(self, show_triggers_node) -> LogicalPlan:
        """创建SHOW TRIGGERS语句的执行计划"""
        show_triggers_op = ShowTriggersOperator()
        return LogicalPlan(show_triggers_op)
    
    def _create_declare_cursor_plan(self, declare_cursor_node) -> LogicalPlan:
        """创建DECLARE CURSOR语句的执行计划"""
        cursor_name = declare_cursor_node.cursor_name.value
        query = declare_cursor_node.query
        
        # 为游标的查询创建执行计划
        query_plan = self.create_plan(query)
        
        declare_cursor_op = DeclareCursorOperator(
            cursor_name=cursor_name,
            query_plan=query_plan.root
        )
        return LogicalPlan(declare_cursor_op)
    
    def _create_open_cursor_plan(self, open_cursor_node) -> LogicalPlan:
        """创建OPEN CURSOR语句的执行计划"""
        cursor_name = open_cursor_node.cursor_name.value
        
        open_cursor_op = OpenCursorOperator(cursor_name=cursor_name)
        return LogicalPlan(open_cursor_op)
    
    def _create_fetch_cursor_plan(self, fetch_cursor_node) -> LogicalPlan:
        """创建FETCH CURSOR语句的执行计划"""
        cursor_name = fetch_cursor_node.cursor_name.value
        
        fetch_cursor_op = FetchCursorOperator(cursor_name=cursor_name)
        return LogicalPlan(fetch_cursor_op)
    
    def _create_close_cursor_plan(self, close_cursor_node) -> LogicalPlan:
        """创建CLOSE CURSOR语句的执行计划"""
        cursor_name = close_cursor_node.cursor_name.value
        
        close_cursor_op = CloseCursorOperator(cursor_name=cursor_name)
        return LogicalPlan(close_cursor_op)
    
    def _create_begin_transaction_plan(self, begin_node) -> LogicalPlan:
        """创建BEGIN TRANSACTION语句的执行计划"""
        # 创建一个特殊的操作符来表示开始事务
        begin_op = BeginTransactionOperator()
        return LogicalPlan(begin_op)
    
    def _create_commit_transaction_plan(self, commit_node) -> LogicalPlan:
        """创建COMMIT TRANSACTION语句的执行计划"""
        # 创建一个特殊的操作符来表示提交事务
        commit_op = CommitTransactionOperator()
        return LogicalPlan(commit_op)
    
    def _create_rollback_transaction_plan(self, rollback_node) -> LogicalPlan:
        """创建ROLLBACK TRANSACTION语句的执行计划"""
        # 创建一个特殊的操作符来表示回滚事务
        rollback_op = RollbackTransactionOperator()
        return LogicalPlan(rollback_op)


class EnhancedExecutionPlanGenerator:
    """增强的执行计划生成器"""
    
    def __init__(self, symbol_table: SymbolTable):
        self.symbol_table = symbol_table
        self.planner = EnhancedQueryPlanner(symbol_table)
