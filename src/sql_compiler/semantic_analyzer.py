# -*- coding: utf-8 -*-
"""
语义分析器 - 进行语义检查和验证
"""
from typing import List, Optional, Dict, Any
from .symbol_table import SymbolTable, TableInfo, ColumnInfo, DataType, TypeChecker
# 旧语法分析器已删除，不再需要导入
import sys
import os

# 添加路径以便导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class SemanticError(Exception):
    """语义错误异常"""
    def __init__(self, message: str, line: int = 0, column: int = 0):
        super().__init__(message)
        self.message = message
        self.line = line
        self.column = column
    
    def __str__(self):
        if self.line > 0:
            return f"Semantic Error at line {self.line}, column {self.column}: {self.message}"
        return f"Semantic Error: {self.message}"


class SemanticAnalyzer:
    """语义分析器"""
    
    def __init__(self, symbol_table: Optional[SymbolTable] = None):
        self.symbol_table = symbol_table or SymbolTable()
        self.errors: List[SemanticError] = []
        self.warnings: List[str] = []
    
    def analyze(self, ast_node) -> bool:
        """
        对AST进行语义分析
        返回True表示分析成功，False表示有错误
        """
        self.errors.clear()
        self.warnings.clear()
        
        try:
            if hasattr(ast_node, 'query'):
                self._analyze_query(ast_node.query)
            else:
                self._analyze_query(ast_node)
            return len(self.errors) == 0
        except Exception as e:
            self._add_error(f"Unexpected error during semantic analysis: {e}")
            return False
    
    def _analyze_query(self, query_node):
        """分析查询节点"""
        # 检查是否是语法分析器的AST格式
        # 先检查INSERT语句（有values属性）
        if hasattr(query_node, 'values'):  # INSERT语句
            self._analyze_insert(query_node)
        # 再检查DELETE语句（有table和where属性，但没有values）
        elif hasattr(query_node, 'table') and hasattr(query_node, 'where') and not hasattr(query_node, 'values'):  # DELETE语句
            self._analyze_delete(query_node)
        # 然后检查CREATE TABLE语句（有table和columns属性，但没有values和where）
        elif hasattr(query_node, 'table') and hasattr(query_node, 'columns') and not hasattr(query_node, 'values') and not hasattr(query_node, 'where'):  # CREATE TABLE语句
            self._analyze_create_table(query_node)
        # 检查DROP TABLE语句（有table属性，但没有columns、values、where）
        elif hasattr(query_node, 'table') and not hasattr(query_node, 'columns') and not hasattr(query_node, 'values') and not hasattr(query_node, 'where'):  # DROP TABLE语句
            self._analyze_drop_table(query_node)
        # 检查SHOW COLUMNS/INDEX语句（有table_name属性）
        elif hasattr(query_node, 'table_name'):  # SHOW COLUMNS/INDEX语句
            self._analyze_show(query_node)
        # 检查SHOW TABLES语句（没有table、columns和table_name属性）
        elif not hasattr(query_node, 'table') and not hasattr(query_node, 'columns') and not hasattr(query_node, 'table_name'):  # SHOW TABLES语句
            self._analyze_show_tables(query_node)
        # 检查EXPLAIN语句（有query属性）
        elif hasattr(query_node, 'query'):  # EXPLAIN语句
            self._analyze_explain(query_node)
        # 检查CREATE INDEX语句（有index_name属性）
        elif hasattr(query_node, 'index_name'):  # CREATE INDEX语句
            self._analyze_create_index(query_node)
        # 最后检查SELECT语句（有columns和table属性）
        elif hasattr(query_node, 'columns') and hasattr(query_node, 'table'):  # SELECT语句
            self._analyze_select(query_node)
        else:
            # 尝试从语法分析器的AST结构中识别
            if hasattr(query_node, '__class__'):
                class_name = query_node.__class__.__name__
                if 'Select' in class_name:
                    self._analyze_select(query_node)
                elif 'Insert' in class_name:
                    self._analyze_insert(query_node)
                elif 'Delete' in class_name:
                    self._analyze_delete(query_node)
                elif 'CreateTable' in class_name:
                    self._analyze_create_table(query_node)
                else:
                    self._add_error(f"Unknown query type: {class_name}")
            else:
                self._add_error(f"Unknown query type: {type(query_node)}")
    
    def _analyze_select(self, select_node):
        """分析SELECT语句"""
        # 1. 检查表是否存在
        # 从from_clause中获取表名
        if hasattr(select_node, 'from_clause') and select_node.from_clause:
            # 处理from_clause，可能包含表名或表引用
            if hasattr(select_node.from_clause, 'name'):
                table_name = select_node.from_clause.name
            elif hasattr(select_node.from_clause, 'table'):
                table_name = select_node.from_clause.table
            else:
                # 简化处理：假设from_clause就是表名
                table_name = str(select_node.from_clause)
        elif hasattr(select_node, 'table') and select_node.table:
            # 兼容旧的table属性
            table_name = select_node.table.name if hasattr(select_node.table, 'name') else str(select_node.table)
        elif hasattr(select_node, 'table_name') and select_node.table_name:
            # 处理TableReference对象
            if hasattr(select_node.table_name, 'table_name'):
                table_name = select_node.table_name.table_name
            else:
                table_name = str(select_node.table_name)
        else:
            self._add_error("No table specified in FROM clause")
            return
            
        if not self.symbol_table.table_exists(table_name):
            self._add_error(f"Table '{table_name}' does not exist")
            return
        
        table_info = self.symbol_table.get_table(table_name)
        
        # 2. 检查列是否存在
        if hasattr(select_node.columns, 'all') and select_node.columns.all:
            # SELECT * - 不需要检查具体列
            pass
        else:
            # SELECT column1, column2, ... - 检查每个列
            if hasattr(select_node.columns, 'names') and select_node.columns.names:
                # 确保names是可迭代的
                try:
                    for column_name in select_node.columns.names:
                        if not self.symbol_table.column_exists(table_name, column_name):
                            self._add_error(f"Column '{column_name}' does not exist in table '{table_name}'")
                except TypeError as e:
                    self._add_error(f"Columns object is not iterable: {e}")
                    return
        
        # 3. 分析WHERE条件
        if select_node.where:
            self._analyze_where_clause(select_node.where, table_info)
        
        # 4. 分析JOIN
        if hasattr(select_node, 'joins') and select_node.joins:
            for join in select_node.joins:
                self._analyze_join(join)
    
    def _analyze_insert(self, insert_node):
        """分析INSERT语句 - 支持多行VALUES"""
        table_name = insert_node.table_name if hasattr(insert_node, 'table_name') else insert_node.table.name
        
        # 1. 检查表是否存在
        if not self.symbol_table.table_exists(table_name):
            self._add_error(f"Table '{table_name}' does not exist")
            return
        
        table_info = self.symbol_table.get_table(table_name)
        
        # 2. 处理多行VALUES
        values_list = insert_node.values
        if not isinstance(values_list, list) or len(values_list) == 0:
            self._add_error("INSERT语句必须包含至少一行VALUES")
            return
        
        # 检查第一行是否是嵌套列表（多行VALUES）
        if isinstance(values_list[0], list):
            # 多行VALUES
            for row_idx, values in enumerate(values_list):
                self._analyze_insert_row(table_name, table_info, insert_node.columns, values, row_idx)
        else:
            # 单行VALUES
            self._analyze_insert_row(table_name, table_info, insert_node.columns, values_list, 0)
    
    def _analyze_insert_row(self, table_name, table_info, provided_columns, values, row_idx):
        """分析INSERT语句的一行数据"""
        # 处理带列名的INSERT语句
        if provided_columns:
            # 带列名的INSERT语句
            if len(provided_columns) != len(values):
                self._add_error(
                    f"Row {row_idx + 1}: Column count mismatch: {len(provided_columns)} columns specified, "
                    f"but {len(values)} values provided"
                )
                return
            
            # 检查每个值与其对应列的类型
            for i, (col_name, value_node) in enumerate(zip(provided_columns, values)):
                # 查找列信息
                column_info = None
                for col in table_info.columns:
                    if col.name == col_name:
                        column_info = col
                        break
                
                if not column_info:
                    self._add_error(f"Row {row_idx + 1}: Column '{col_name}' does not exist in table '{table_name}'")
                    continue
                
                # 检查类型兼容性
                self._check_value_type_compatibility(value_node, column_info)
        else:
            # 不带列名的INSERT语句（按位置匹配）
            expected_columns = len(table_info.columns)
            provided_values = len(values)
            
            if expected_columns != provided_values:
                self._add_error(
                    f"Row {row_idx + 1}: Column count mismatch: table '{table_name}' has {expected_columns} columns, "
                    f"but {provided_values} values provided"
                )
                return
            
            # 检查值类型
            for i, value_node in enumerate(values):
                if i < len(table_info.columns):
                    column_info = table_info.columns[i]
                    self._check_value_type_compatibility(value_node, column_info)
    
    def _analyze_where_clause(self, where_node, table_info: TableInfo):
        """分析WHERE子句"""
        self._analyze_expression(where_node, table_info)
    
    def _analyze_expression(self, expr_node, table_info: TableInfo):
        """分析表达式"""
        if hasattr(expr_node, 'left') and hasattr(expr_node, 'right'):
            # 二元表达式
            self._analyze_expression(expr_node.left, table_info)
            self._analyze_expression(expr_node.right, table_info)
            
            # 检查操作符两边的类型兼容性
            left_type = self._get_expression_type(expr_node.left, table_info)
            right_type = self._get_expression_type(expr_node.right, table_info)
            
            if left_type and right_type:
                if not TypeChecker.are_compatible(left_type, right_type):
                    # 获取操作符，兼容不同的属性名
                    operator = getattr(expr_node, 'op', getattr(expr_node, 'operator', 'unknown'))
                    self._add_error(
                        f"Type mismatch in expression: {left_type.value} and {right_type.value} "
                        f"are not compatible for operation '{operator}'"
                    )
        elif hasattr(expr_node, 'name'):
            # 标识符 - 检查列是否存在
            column_name = expr_node.name
            if not self.symbol_table.column_exists(table_info.name, column_name):
                self._add_error(f"Column '{column_name}' does not exist in table '{table_info.name}'")
        elif hasattr(expr_node, 'value'):
            # 字面量 - 不需要额外检查
            pass
    
    def _get_expression_type(self, expr_node, table_info: TableInfo) -> Optional[DataType]:
        """获取表达式的类型"""
        if hasattr(expr_node, 'name'):
            # 标识符 - 从表中获取列类型
            column_info = table_info.get_column(expr_node.name)
            return column_info.data_type if column_info else None
        elif hasattr(expr_node, 'value'):
            # 字面量 - 推断类型
            return TypeChecker.get_literal_type(str(expr_node.value))
        return None
    
    def _check_value_type_compatibility(self, value_node, column_info: ColumnInfo):
        """检查值类型与列类型的兼容性"""
        try:
            value_type = TypeChecker.get_literal_type(str(value_node.value))
            if value_type and not TypeChecker.are_compatible(value_type, column_info.data_type):
                self._add_error(
                    f"Type mismatch: value '{value_node.value}' (type {value_type.value}) "
                    f"is not compatible with column '{column_info.name}' (type {column_info.data_type.value})"
                )
        except AttributeError as e:
            self._add_error(f"Value type check error: {e}, value_node = {value_node}")
    
    def _add_error(self, message: str, line: int = 0, column: int = 0):
        """添加错误"""
        self.errors.append(SemanticError(message, line, column))
    
    def _add_warning(self, message: str):
        """添加警告"""
        self.warnings.append(message)
    
    def get_errors(self) -> List[SemanticError]:
        """获取所有错误"""
        return self.errors.copy()
    
    def get_warnings(self) -> List[str]:
        """获取所有警告"""
        return self.warnings.copy()
    
    def has_errors(self) -> bool:
        """检查是否有错误"""
        return len(self.errors) > 0
    
    def _analyze_delete(self, delete_node):
        """分析DELETE语句"""
        # 1. 检查表是否存在
        if hasattr(delete_node, 'table') and hasattr(delete_node.table, 'name'):
            table_name = delete_node.table.name
        else:
            self._add_error("Invalid DELETE statement: missing table name")
            return
            
        if not self.symbol_table.table_exists(table_name):
            self._add_error(f"Table '{table_name}' does not exist")
            return
        
        # 2. 如果有WHERE子句，分析条件
        if hasattr(delete_node, 'where') and delete_node.where:
            table_info = self.symbol_table.get_table(table_name)
            if table_info:
                self._analyze_expression(delete_node.where, table_info)
    
    def _analyze_create_table(self, create_table_node):
        """分析CREATE TABLE语句"""
        # CREATE TABLE语句在语义分析阶段通常不需要特殊检查
        # 因为表还不存在，主要是语法检查
        table_name = create_table_node.table.name
        
        # 注意：在语义分析阶段，我们不检查表是否已存在
        # 因为CREATE TABLE语句的目的是创建新表
        # 如果表已存在，这应该在执行阶段处理，而不是语义分析阶段
        
        # 检查列定义是否有效
        if hasattr(create_table_node, 'columns') and create_table_node.columns:
            col_names = []
            # 确保columns是可迭代的
            try:
                # 检查columns是否是列表
                if isinstance(create_table_node.columns, list):
                    for col_def in create_table_node.columns:
                        if hasattr(col_def, 'name') and hasattr(col_def, 'data_type'):
                            # 检查列名是否重复
                            if col_def.name in col_names:
                                self._add_error(f"Duplicate column name '{col_def.name}' in table '{table_name}'")
                                return
                            col_names.append(col_def.name)
                else:
                    # 如果不是列表，跳过列名重复检查
                    pass
            except TypeError as e:
                self._add_error(f"Columns object is not iterable in CREATE TABLE: {e}")
                return

    def _analyze_drop_table(self, drop_table_node):
        """分析DROP TABLE语句"""
        table_name = drop_table_node.table.name
        
        # 检查表是否存在
        if not self.symbol_table.table_exists(table_name):
            self._add_error(f"Table '{table_name}' does not exist")
            return
        
        # DROP TABLE语句没有其他需要验证的内容
        self._add_warning(f"Table '{table_name}' will be dropped")
    
    def _analyze_show_tables(self, show_tables_node):
        """分析SHOW TABLES语句"""
        # SHOW TABLES不需要特殊验证，直接返回
        return True
    
    def _analyze_show(self, show_node):
        """分析SHOW COLUMNS/INDEX语句"""
        table_name = show_node.table_name
        
        # 检查表是否存在
        if not self.symbol_table.table_exists(table_name):
            self._add_error(f"Table '{table_name}' does not exist")
            return
        
        return True
    
    def _analyze_explain(self, explain_node):
        """分析EXPLAIN语句"""
        # 递归分析要解释的查询
        if hasattr(explain_node, 'query'):
            self._analyze_query(explain_node.query)
        return True
    
    def _analyze_create_index(self, create_index_node):
        """分析CREATE INDEX语句"""
        table_name = create_index_node.table_name
        column_name = create_index_node.column_name
        
        # 检查表是否存在
        if not self.symbol_table.table_exists(table_name):
            self._add_error(f"Table '{table_name}' does not exist")
            return
        
        # 检查列是否存在
        table_info = self.symbol_table.get_table_info(table_name)
        column_exists = any(col.column_name == column_name for col in table_info.columns)
        if not column_exists:
            self._add_error(f"Column '{column_name}' does not exist in table '{table_name}'")
            return
        
        return True
    
    def _analyze_join(self, join_node):
        """分析JOIN语句"""
        # 检查右表是否存在
        right_table_name = join_node.right_table.table_name if hasattr(join_node.right_table, 'table_name') else str(join_node.right_table)
        if not self.symbol_table.table_exists(right_table_name):
            self._add_error(f"Table '{right_table_name}' does not exist in JOIN")
            return
        
        # 检查连接条件中的列是否存在
        condition = join_node.condition
        if hasattr(condition, 'left_table') and hasattr(condition, 'left_column'):
            if not self.symbol_table.column_exists(condition.left_table, condition.left_column):
                self._add_error(f"Column '{condition.left_table}.{condition.left_column}' does not exist in JOIN condition")
        
        if hasattr(condition, 'right_table') and hasattr(condition, 'right_column'):
            if not self.symbol_table.column_exists(condition.right_table, condition.right_column):
                self._add_error(f"Column '{condition.right_table}.{condition.right_column}' does not exist in JOIN condition")
        
        return True


# 预定义一些测试表结构
def create_sample_symbol_table() -> SymbolTable:
    """创建包含示例表的符号表"""
    symbol_table = SymbolTable()
    
    # 创建用户表
    users_table = TableInfo("users")
    users_table.columns = [
        ColumnInfo("id", DataType.INT, is_primary_key=True),
        ColumnInfo("name", DataType.VARCHAR),
        ColumnInfo("age", DataType.INT),
        ColumnInfo("email", DataType.VARCHAR),
        ColumnInfo("created_at", DataType.TIMESTAMP)
    ]
    symbol_table.add_table(users_table)
    
    # 创建产品表
    products_table = TableInfo("products")
    products_table.columns = [
        ColumnInfo("id", DataType.INT, is_primary_key=True),
        ColumnInfo("name", DataType.VARCHAR),
        ColumnInfo("price", DataType.DECIMAL),
        ColumnInfo("category", DataType.VARCHAR),
        ColumnInfo("in_stock", DataType.BOOLEAN)
    ]
    symbol_table.add_table(products_table)
    
    return symbol_table