# -*- coding: utf-8 -*-
"""
简化AST转换器 - 将新语法分析器的AST转换为现有系统兼容的格式
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from .new_syntax_analyzer import *
from .ast_nodes import *


class SimpleASTConverter:
    """简化AST转换器 - 转换新语法分析器的AST为兼容格式"""
    
    def convert(self, new_ast):
        """转换新AST为兼容格式"""
        if isinstance(new_ast, Program):
            return self._convert_program(new_ast)
        else:
            return new_ast
    
    def _convert_program(self, program):
        """转换Program节点"""
        query = self._convert_query(program.query)
        return Program(query)
    
    def _convert_query(self, query):
        """转换Query节点"""
        if isinstance(query, CreateTable):
            return self._convert_create_table(query)
        elif isinstance(query, Insert):
            return self._convert_insert(query)
        elif isinstance(query, Select):
            return self._convert_select(query)
        elif isinstance(query, Update):
            return self._convert_update(query)
        elif isinstance(query, Delete):
            return self._convert_delete(query)
        elif isinstance(query, DropTable):
            return self._convert_drop_table(query)
        elif isinstance(query, ShowTables):
            return self._convert_show_tables(query)
        elif isinstance(query, ShowColumns):
            return self._convert_show_columns(query)
        elif isinstance(query, ShowIndex):
            return self._convert_show_index(query)
        elif isinstance(query, Explain):
            return self._convert_explain(query)
        elif isinstance(query, CreateIndex):
            return self._convert_create_index(query)
        else:
            return query
    
    def _convert_create_table(self, create_table):
        """转换CreateTable节点"""
        # 创建一个兼容的CreateTable对象
        class CompatibleCreateTable:
            def __init__(self, table_name, columns):
                self.table = type('Table', (), {'name': table_name})()
                self.columns = columns
        
        # 转换列定义
        columns = []
        for col_def in create_table.columns:
            column = self._convert_column_definition(col_def)
            columns.append(column)
        
        return CompatibleCreateTable(create_table.table_name, columns)
    
    def _convert_column_definition(self, col_def):
        """转换ColumnDefinition节点"""
        # 创建一个兼容的ColumnDef对象
        class CompatibleColumnDef:
            def __init__(self, name, datatype):
                self.name = name
                self.datatype = datatype
        
        # 转换数据类型
        data_type = self._convert_data_type(col_def.data_type)
        
        # 处理列名，可能是Identifier对象或字符串
        if hasattr(col_def.name, 'value'):
            column_name = col_def.name.value
        else:
            column_name = col_def.name
        
        return CompatibleColumnDef(column_name, data_type)
    
    def _convert_data_type(self, data_type):
        """转换DataType节点"""
        if data_type.arg2 is not None:
            # DECIMAL类型，有两个参数
            return f"{data_type.type_name}({data_type.arg1},{data_type.arg2})"
        elif data_type.arg1 is not None:
            # VARCHAR类型，有一个参数
            return f"{data_type.type_name}({data_type.arg1})"
        else:
            # 基本类型，无参数
            return data_type.type_name
    
    def _convert_insert(self, insert):
        """转换Insert节点"""
        # 创建一个兼容的Insert对象
        class CompatibleInsert:
            def __init__(self, table_name, values, columns=None):
                self.table_name = table_name  # 添加table_name属性
                self.table = type('Table', (), {'name': table_name})()
                self.values = values
                self.columns = columns
        
        # 转换值列表 - 支持多行VALUES
        values = []
        
        # 检查是否为多行VALUES（嵌套列表）
        if isinstance(insert.values, list) and len(insert.values) > 0 and isinstance(insert.values[0], list):
            # 多行VALUES格式：[[row1], [row2], ...]
            for row in insert.values:
                row_values = []
                for value in row:
                    if isinstance(value, Value):
                        if value.type_name == 'NUMBER':
                            row_values.append(Literal(Token('NUMBER', str(value.value), 0, 0), value.value))
                        elif value.type_name == 'STRING':
                            row_values.append(Literal(Token('STRING', value.value, 0, 0), value.value))
                        else:
                            row_values.append(Literal(Token('STRING', str(value.value), 0, 0), str(value.value)))
                    else:
                        row_values.append(Literal(Token('STRING', str(value), 0, 0), str(value)))
                values.append(row_values)
        else:
            # 单行VALUES格式：[val1, val2, ...]
            for value in insert.values:
                if isinstance(value, Value):
                    if value.type_name == 'NUMBER':
                        values.append(Literal(Token('NUMBER', str(value.value), 0, 0), value.value))
                    elif value.type_name == 'STRING':
                        values.append(Literal(Token('STRING', value.value, 0, 0), value.value))
                    else:
                        values.append(Literal(Token('STRING', str(value.value), 0, 0), str(value.value)))
                else:
                    values.append(Literal(Token('STRING', str(value), 0, 0), str(value)))
        
        return CompatibleInsert(insert.table_name, values, insert.columns)
    
    def _convert_select(self, select):
        """转换Select节点"""
        # 创建一个兼容的Select对象
        class CompatibleSelect:
            def __init__(self, columns, table_name, where=None, order_by=None, joins=None, group_by=None, limit=None):
                self.columns = columns
                # 检查table_name是否是SubqueryReference对象
                if hasattr(table_name, 'subquery') or "SubqueryReference" in str(type(table_name)):
                    # 保持SubqueryReference对象不变
                    self.table = table_name
                elif hasattr(table_name, 'tables') or "MultiTableReference" in str(type(table_name)):
                    # 保持MultiTableReference对象不变
                    self.table = table_name
                else:
                    # 普通表名，转换为Table对象
                    self.table = type('Table', (), {'name': table_name})()
                self.where = where
                self.order_by = order_by
                self.joins = joins or []
                self.group_by = group_by
                self.limit = limit
        
        # 转换列列表
        if select.columns == ['*']:
            columns = [Identifier(Token('IDENTIFIER', '*', 0, 0), '*')]
        else:
            columns = []
            for col in select.columns:
                if isinstance(col, str):
                    # 普通列名
                    columns.append(Identifier(Token('IDENTIFIER', col, 0, 0), col))
                elif isinstance(col, AggregateFunction):
                    # 聚合函数
                    columns.append(col)
                elif hasattr(col, 'column_name') and hasattr(col, 'alias'):
                    # ColumnWithAlias对象
                    columns.append(col)
                else:
                    # 其他类型，转换为字符串
                    columns.append(Identifier(Token('IDENTIFIER', str(col), 0, 0), str(col)))
        
        # 转换WHERE条件
        where = None
        if select.where_clause:
            where = self._convert_condition(select.where_clause)
        
        # 转换ORDER BY
        order_by = None
        if select.order_by:
            order_by = self._convert_order_by(select.order_by)
        
        # 转换GROUP BY
        group_by = None
        if hasattr(select, 'group_by') and select.group_by:
            group_by = self._convert_group_by(select.group_by)
        
        # 转换JOIN
        joins = []
        if hasattr(select, 'joins') and select.joins:
            for join in select.joins:
                joins.append(self._convert_join(join))
        
        # 处理表名
        table_name = select.table_name
        if hasattr(table_name, 'table_name'):
            table_name = table_name.table_name
        
        # 转换limit
        limit = None
        if hasattr(select, 'limit') and select.limit:
            limit = self._convert_limit(select.limit)
        
        return CompatibleSelect(columns, table_name, where, order_by, joins, group_by, limit)
    
    def _convert_condition(self, condition):
        """转换Condition节点"""
        # 检查是否是BETWEEN条件
        if hasattr(condition, 'value1') and hasattr(condition, 'value2'):
            # 这是BetweenCondition
            return self._convert_between_condition(condition)
        elif hasattr(condition, 'left') and hasattr(condition, 'right') and hasattr(condition, 'op'):
            # 检查是AndCondition还是OrCondition
            if condition.op == 'AND':
                return self._convert_and_condition(condition)
            elif condition.op == 'OR':
                return self._convert_or_condition(condition)
            else:
                # 其他逻辑操作符
                return self._convert_and_condition(condition)
        else:
            # 这是普通Condition
            return self._convert_simple_condition(condition)
    
    def _convert_simple_condition(self, condition):
        """转换普通Condition节点"""
        # 转换左操作数
        left = Identifier(Token('IDENTIFIER', condition.left, 0, 0), condition.left)
        
        # 转换右操作数
        if isinstance(condition.right, Value):
            if condition.right.type_name == 'NUMBER':
                right = Literal(Token('NUMBER', str(condition.right.value), 0, 0), condition.right.value)
            elif condition.right.type_name == 'STRING':
                right = Literal(Token('STRING', condition.right.value, 0, 0), condition.right.value)
            else:
                right = Literal(Token('STRING', str(condition.right.value), 0, 0), str(condition.right.value))
        else:
            right = Literal(Token('STRING', str(condition.right), 0, 0), str(condition.right))
        
        # 创建比较表达式
        if condition.operator == '=':
            return BinaryExpr(left, Token('=', '=', 0, 0), right)
        elif condition.operator == '>':
            return BinaryExpr(left, Token('>', '>', 0, 0), right)
        elif condition.operator == '<':
            return BinaryExpr(left, Token('<', '<', 0, 0), right)
        elif condition.operator == '>=':
            return BinaryExpr(left, Token('>=', '>=', 0, 0), right)
        elif condition.operator == '<=':
            return BinaryExpr(left, Token('<=', '<=', 0, 0), right)
        elif condition.operator == '!=':
            return BinaryExpr(left, Token('!=', '!=', 0, 0), right)
        else:
            return BinaryExpr(left, Token('=', '=', 0, 0), right)
    
    def _convert_between_condition(self, condition):
        """转换BetweenCondition节点"""
        # 转换左操作数
        left = Identifier(Token('IDENTIFIER', condition.left, 0, 0), condition.left)
        
        # 转换value1
        if isinstance(condition.value1, Value):
            if condition.value1.type_name == 'NUMBER':
                value1 = Literal(Token('NUMBER', str(condition.value1.value), 0, 0), condition.value1.value)
            elif condition.value1.type_name == 'STRING':
                value1 = Literal(Token('STRING', condition.value1.value, 0, 0), condition.value1.value)
            else:
                value1 = Literal(Token('STRING', str(condition.value1.value), 0, 0), str(condition.value1.value))
        else:
            value1 = Literal(Token('STRING', str(condition.value1), 0, 0), str(condition.value1))
        
        # 转换value2
        if isinstance(condition.value2, Value):
            if condition.value2.type_name == 'NUMBER':
                value2 = Literal(Token('NUMBER', str(condition.value2.value), 0, 0), condition.value2.value)
            elif condition.value2.type_name == 'STRING':
                value2 = Literal(Token('STRING', condition.value2.value, 0, 0), condition.value2.value)
            else:
                value2 = Literal(Token('STRING', str(condition.value2.value), 0, 0), str(condition.value2.value))
        else:
            value2 = Literal(Token('STRING', str(condition.value2), 0, 0), str(condition.value2))
        
        # 创建BETWEEN表达式 (left >= value1 AND left <= value2)
        left_ge_value1 = BinaryExpr(left, Token('>=', '>=', 0, 0), value1)
        left_le_value2 = BinaryExpr(left, Token('<=', '<=', 0, 0), value2)
        
        # 创建AND表达式
        return BinaryExpr(left_ge_value1, Token('AND', 'AND', 0, 0), left_le_value2)
    
    def _convert_and_condition(self, condition):
        """转换AndCondition节点"""
        # 递归转换左右条件
        left_condition = self._convert_condition(condition.left)
        right_condition = self._convert_condition(condition.right)
        
        # 创建AND表达式
        return BinaryExpr(left_condition, Token('AND', 'AND', 0, 0), right_condition)
    
    def _convert_or_condition(self, condition):
        """转换OrCondition节点"""
        # 递归转换左右条件
        left_condition = self._convert_condition(condition.left)
        right_condition = self._convert_condition(condition.right)
        
        # 创建OR表达式
        return BinaryExpr(left_condition, Token('OR', 'OR', 0, 0), right_condition)
    
    def _convert_order_by(self, order_by):
        """转换OrderBy节点"""
        # 创建一个兼容的OrderBy对象
        class CompatibleOrderBy:
            def __init__(self, column, direction):
                self.column = column
                self.direction = direction
        
        # 创建一个兼容的OrderByList对象
        class CompatibleOrderByList:
            def __init__(self, order_columns):
                self.order_columns = order_columns
        
        # 检查是否是OrderByList
        if hasattr(order_by, 'order_columns'):
            # 这是OrderByList，转换所有排序列
            converted_columns = []
            for order_col in order_by.order_columns:
                converted_col = CompatibleOrderBy(
                    Identifier(Token('IDENTIFIER', order_col.column, 0, 0), order_col.column), 
                    order_col.direction
                )
                converted_columns.append(converted_col)
            return CompatibleOrderByList(converted_columns)
        else:
            # 这是单个OrderBy
            return CompatibleOrderBy(Identifier(Token('IDENTIFIER', order_by.column, 0, 0), order_by.column), order_by.direction)
    
    def _convert_limit(self, limit):
        """转换Limit节点"""
        class CompatibleLimit:
            def __init__(self, limit, offset=0):
                self.limit = limit
                self.offset = offset
        
        return CompatibleLimit(limit.limit, getattr(limit, 'offset', 0))
    
    def _convert_group_by(self, group_by):
        """转换GroupBy节点"""
        # 直接返回GroupBy对象，因为查询规划器可以直接使用
        return group_by
    
    def _convert_update(self, update):
        """转换Update节点"""
        # 创建一个兼容的Update对象
        class CompatibleUpdate:
            def __init__(self, table_name, assignments, where=None):
                self.table = type('Table', (), {'name': table_name})()
                self.assignments = assignments
                self.set_list = assignments  # 添加set_list属性以兼容查询规划器
                self.where = where
        
        # 转换赋值列表
        assignments = []
        for assignment in update.assignments:
            col = Identifier(Token('IDENTIFIER', assignment.column, 0, 0), assignment.column)
            if isinstance(assignment.value, Value):
                if assignment.value.type_name == 'NUMBER':
                    val = Literal(Token('NUMBER', str(assignment.value.value), 0, 0), assignment.value.value)
                elif assignment.value.type_name == 'STRING':
                    val = Literal(Token('STRING', assignment.value.value, 0, 0), assignment.value.value)
                else:
                    val = Literal(Token('STRING', str(assignment.value.value), 0, 0), str(assignment.value.value))
            elif hasattr(assignment.value, 'left') and hasattr(assignment.value, 'operator') and hasattr(assignment.value, 'right'):
                # 处理BinaryExpr对象，直接传递而不转换为Literal
                val = assignment.value
            else:
                val = Literal(Token('STRING', str(assignment.value), 0, 0), str(assignment.value))
            
            assignments.append(Assignment(col, val))
        
        # 转换WHERE条件
        where = None
        if update.where_clause:
            where = self._convert_condition(update.where_clause)
        
        return CompatibleUpdate(update.table_name, assignments, where)
    
    def _convert_delete(self, delete):
        """转换Delete节点"""
        # 创建一个兼容的Delete对象
        class CompatibleDelete:
            def __init__(self, table_name, where=None):
                self.table = type('Table', (), {'name': table_name})()
                self.where = where
        
        # 转换WHERE条件
        where = None
        if delete.where_clause:
            where = self._convert_condition(delete.where_clause)
        
        return CompatibleDelete(delete.table_name, where)
    
    def _convert_drop_table(self, drop_table):
        """转换DropTable节点"""
        # 创建一个兼容的DropTable对象
        class CompatibleDropTable:
            def __init__(self, table_name, if_exists=False):
                self.table = type('Table', (), {'name': table_name})()
                self.if_exists = if_exists
        
        return CompatibleDropTable(drop_table.table_name, drop_table.if_exists)
    
    def _convert_show_tables(self, show_tables):
        """转换ShowTables节点"""
        class CompatibleShowTables:
            def __init__(self):
                pass
        
        return CompatibleShowTables()
    
    def _convert_show_columns(self, show_columns):
        """转换ShowColumns节点"""
        class CompatibleShowColumns:
            def __init__(self, table_name):
                self.table_name = table_name
        
        return CompatibleShowColumns(show_columns.table_name)
    
    def _convert_show_index(self, show_index):
        """转换ShowIndex节点"""
        class CompatibleShowIndex:
            def __init__(self, table_name):
                self.table_name = table_name
        
        return CompatibleShowIndex(show_index.table_name)
    
    def _convert_explain(self, explain):
        """转换Explain节点"""
        class CompatibleExplain:
            def __init__(self, query):
                self.query = query
        
        return CompatibleExplain(explain.query)
    
    def _convert_create_index(self, create_index):
        """转换CreateIndex节点"""
        class CompatibleCreateIndex:
            def __init__(self, index_name, table_name, column_or_columns):
                self.index_name = index_name
                self.table_name = table_name
                # 支持单列或多列
                if isinstance(column_or_columns, list):
                    self.columns = column_or_columns
                    self.column_name = column_or_columns[0]  # 保持向后兼容性
                else:
                    self.column_name = column_or_columns
                    self.columns = [column_or_columns]
        
        # 获取列信息（支持多列）
        if hasattr(create_index, 'columns'):
            # 多列索引
            columns = create_index.columns
        else:
            # 单列索引（向后兼容）
            columns = create_index.column_name
        
        return CompatibleCreateIndex(
            create_index.index_name,
            create_index.table_name,
            columns
        )
    
    def _convert_join(self, join):
        """转换Join节点"""
        class CompatibleJoin:
            def __init__(self, join_type, right_table, condition):
                self.join_type = join_type
                self.right_table = right_table
                self.condition = condition
        
        # 转换右表
        right_table = type('Table', (), {'name': join.right_table.table_name, 'table_name': join.right_table.table_name})()
        
        # 转换连接条件
        condition = self._convert_join_condition(join.condition)
        
        return CompatibleJoin(join.join_type, right_table, condition)
    
    def _convert_join_condition(self, join_condition):
        """转换JoinCondition节点"""
        class CompatibleJoinCondition:
            def __init__(self, left_table, left_column, operator, right_table, right_column):
                self.left_table = left_table
                self.left_column = left_column
                self.operator = operator
                self.right_table = right_table
                self.right_column = right_column
        
        return CompatibleJoinCondition(
            join_condition.left_table,
            join_condition.left_column,
            join_condition.operator,
            join_condition.right_table,
            join_condition.right_column
        )
