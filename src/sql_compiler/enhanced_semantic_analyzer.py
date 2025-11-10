# -*- coding: utf-8 -*-
"""
增强的语义分析器 - 修复CREATE TABLE问题并增强功能
"""
from typing import List, Optional, Dict, Any
from .symbol_table import SymbolTable, DataType, ColumnInfo, TableInfo
import sys
import os

# 添加路径以便导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class SemanticError(Exception):
    """语义错误"""
    pass


class SemanticAnalyzer:
    """基础语义分析器"""
    
    def __init__(self, symbol_table: Optional[SymbolTable] = None):
        self.symbol_table = symbol_table or SymbolTable()
        self.errors = []
        self.warnings = []
    
    def analyze(self, ast) -> bool:
        """分析AST"""
        self.errors.clear()
        self.warnings.clear()
        
        try:
            self._analyze_node(ast)
            return len(self.errors) == 0
        except Exception as e:
            self._add_error(f"语义分析异常: {str(e)}")
            return False
    
    def _analyze_node(self, node):
        """分析AST节点"""
        if hasattr(node, 'type'):
            if node.type == 'CreateTableStatement':
                self._analyze_create_table(node)
            elif node.type == 'SelectStatement':
                self._analyze_select(node)
            elif node.type == 'InsertStatement':
                self._analyze_insert(node)
            elif node.type == 'UpdateStatement':
                self._analyze_update(node)
            elif node.type == 'DeleteStatement':
                self._analyze_delete(node)
    
    def _analyze_create_table(self, create_table_node):
        """分析CREATE TABLE语句"""
        # 基础实现，子类可以重写
        pass
    
    def _analyze_select(self, select_node):
        """分析SELECT语句"""
        # 基础实现，子类可以重写
        pass
    
    def _analyze_insert(self, insert_node):
        """分析INSERT语句"""
        # 基础实现，子类可以重写
        pass
    
    def _analyze_update(self, update_node):
        """分析UPDATE语句"""
        # 基础实现，子类可以重写
        pass
    
    def _analyze_delete(self, delete_node):
        """分析DELETE语句"""
        # 基础实现，子类可以重写
        pass
    
    def _add_error(self, message: str):
        """添加错误"""
        self.errors.append(SemanticError(message))
    
    def _add_warning(self, message: str):
        """添加警告"""
        self.warnings.append(message)
    
    def get_errors(self) -> List[SemanticError]:
        """获取错误列表"""
        return self.errors
    
    def get_warnings(self) -> List[str]:
        """获取警告列表"""
        return self.warnings


class EnhancedSemanticAnalyzer(SemanticAnalyzer):
    """增强的语义分析器"""
    
    def __init__(self, symbol_table: Optional[SymbolTable] = None):
        super().__init__(symbol_table)
        self.type_checker = TypeChecker()
    
    def _analyze_create_table(self, create_table_node):
        """分析CREATE TABLE语句 - 支持主键约束"""
        table_name = create_table_node.table.name
        
        # 检查表是否已存在
        if self.symbol_table.table_exists(table_name):
            self._add_error(f"Table '{table_name}' already exists")
            return
        
        # 检查列定义
        if not hasattr(create_table_node, 'columns') or not create_table_node.columns:
            self._add_error("CREATE TABLE must have at least one column")
            return
        
        column_infos = []
        column_names = set()
        primary_key_columns = []
        
        # 1. 处理列定义和列级主键约束
        for col_def in create_table_node.columns:
            # 获取列名
            if hasattr(col_def, 'name'):
                if hasattr(col_def.name, 'name'):
                    col_name = col_def.name.name
                else:
                    col_name = str(col_def.name)
            else:
                self._add_error("Column definition must have a name")
                continue
            
            if col_name in column_names:
                self._add_error(f"Duplicate column name: {col_name}")
                continue
            
            column_names.add(col_name)
            
            # 处理数据类型
            col_type = getattr(col_def, 'data_type', None)
            if col_type:
                # 支持不同的数据类型表示方式
                if hasattr(col_type, 'type_name'):
                    data_type_str = col_type.type_name
                elif hasattr(col_type, 'type'):
                    data_type_str = col_type.type
                elif hasattr(col_type, 'name'):
                    data_type_str = col_type.name
                else:
                    data_type_str = str(col_type)
                
                # 转换为标准数据类型
                data_type = self._convert_to_data_type(data_type_str)
                
                # 检查数据类型是否有效
                if data_type == DataType.UNKNOWN:
                    self._add_error(f"Unknown data type: {data_type_str}")
                    continue
            else:
                self._add_error(f"Column '{col_name}' must have a data type")
                continue
            
            # 处理列级约束
            is_primary_key = False
            is_not_null = False
            
            if hasattr(col_def, 'constraints') and col_def.constraints:
                for constraint in col_def.constraints:
                    if constraint == 'PRIMARY KEY':
                        is_primary_key = True
                        primary_key_columns.append(col_name)
                    elif constraint == 'NOT NULL':
                        is_not_null = True
            
            # 主键列不能为NULL
            if is_primary_key:
                is_not_null = True
            
            # 创建列信息
            column_info = ColumnInfo(
                name=col_name,
                data_type=data_type,
                nullable=not is_not_null,
                is_primary_key=is_primary_key
            )
            column_infos.append(column_info)
        
        # 2. 处理表级主键约束
        if hasattr(create_table_node, 'constraints') and create_table_node.constraints:
            for constraint in create_table_node.constraints:
                if hasattr(constraint, '__class__') and 'PrimaryKeyConstraint' in constraint.__class__.__name__:
                    # 处理表级主键约束
                    if hasattr(constraint, 'column_names'):
                        for col_name_obj in constraint.column_names:
                            if hasattr(col_name_obj, 'name'):
                                col_name = col_name_obj.name
                            else:
                                col_name = str(col_name_obj)
                            
                            if col_name not in column_names:
                                self._add_error(f"Primary key column '{col_name}' does not exist")
                                continue
                            
                            if col_name not in primary_key_columns:
                                primary_key_columns.append(col_name)
                            
                            # 更新列信息
                            for col_info in column_infos:
                                if col_info.name == col_name:
                                    col_info.is_primary_key = True
                                    col_info.nullable = False  # 主键不能为NULL
                                    break
        
        # 3. 验证主键约束
        if not primary_key_columns:
            self._add_error("Table must have at least one primary key")
            return
        elif len(primary_key_columns) > 1:
            self._add_error("Current version only supports single column primary key")
            return
        
        if not column_infos:
            self._add_error("No valid columns defined")
            return
        
        # 创建表信息并添加到符号表
        table_info = TableInfo(table_name, column_infos)
        self.symbol_table.add_table(table_info)
    
    def _convert_to_data_type(self, type_str: str) -> DataType:
        """将字符串转换为数据类型"""
        type_mapping = {
            'INT': DataType.INT,
            'INTEGER': DataType.INT,
            'SMALLINT': DataType.INT,
            'BIGINT': DataType.INT,
            'TINYINT': DataType.INT,
            'FLOAT': DataType.FLOAT,
            'REAL': DataType.FLOAT,
            'DOUBLE': DataType.FLOAT,
            'DECIMAL': DataType.DECIMAL,
            'NUMERIC': DataType.DECIMAL,
            'VARCHAR': DataType.VARCHAR,
            'CHAR': DataType.VARCHAR,
            'TEXT': DataType.VARCHAR,
            'DATE': DataType.DATE,
            'TIME': DataType.TIME,
            'TIMESTAMP': DataType.TIMESTAMP,
            'BOOLEAN': DataType.BOOLEAN,
            'BOOL': DataType.BOOLEAN
        }
        
        # 处理带参数的类型，如VARCHAR(50)
        base_type = type_str.split('(')[0].upper()
        return type_mapping.get(base_type, DataType.UNKNOWN)
    
    def _analyze_select(self, select_node):
        """增强的SELECT语句分析"""
        # 调用父类方法
        super()._analyze_select(select_node)
        
        # 检查聚合函数
        if hasattr(select_node, 'columns'):
            self._check_aggregate_functions(select_node.columns)
        
        # 检查GROUP BY
        if hasattr(select_node, 'group_by'):
            self._check_group_by(select_node)
    
    def _check_aggregate_functions(self, columns):
        """检查聚合函数"""
        aggregate_functions = {'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT'}
        
        # 处理不同的columns格式
        if hasattr(columns, 'names') and columns.names:
            # Columns对象，获取names列表
            column_list = columns.names
        elif hasattr(columns, '__iter__'):
            # 可迭代对象
            column_list = columns
        else:
            # 单个对象
            column_list = [columns]
        
        for col in column_list:
            if hasattr(col, 'function') and col.function:
                func_name = col.function.upper()
                if func_name not in aggregate_functions:
                    self._add_error(f"Unknown aggregate function: {func_name}")
    
    def _check_group_by(self, select_node):
        """检查GROUP BY子句"""
        if not hasattr(select_node, 'group_by') or not select_node.group_by:
            return
        
        table_name = select_node.table.name
        table_info = self.symbol_table.get_table(table_name)
        
        if not table_info:
            return
        
        # 检查GROUP BY中的列是否存在
        for col_name in select_node.group_by:
            if not any(col.name == col_name for col in table_info.columns):
                self._add_error(f"Column '{col_name}' in GROUP BY does not exist in table '{table_name}'")


class TypeChecker:
    """类型检查器"""
    
    def __init__(self):
        self.type_rules = TypeRules()
    
    def check_type_compatibility(self, left_type: DataType, right_type: DataType, operator: str) -> bool:
        """检查类型兼容性"""
        if left_type == right_type:
            return True
        
        # 检查数值类型兼容性
        if self._is_numeric_type(left_type) and self._is_numeric_type(right_type):
            return True
        
        # 检查字符串类型兼容性
        if self._is_string_type(left_type) and self._is_string_type(right_type):
            return True
        
        return False
    
    def _is_numeric_type(self, data_type: DataType) -> bool:
        """检查是否为数值类型"""
        return data_type in [DataType.INT, DataType.FLOAT, DataType.DECIMAL]
    
    def _is_string_type(self, data_type: DataType) -> bool:
        """检查是否为字符串类型"""
        return data_type in [DataType.VARCHAR, DataType.CHAR, DataType.TEXT]


class TypeRules:
    """类型规则"""
    
    def __init__(self):
        self.conversion_rules = {
            (DataType.INT, DataType.FLOAT): self._int_to_float,
            (DataType.FLOAT, DataType.INT): self._float_to_int,
            (DataType.VARCHAR, DataType.INT): self._varchar_to_int,
        }
    
    def get_result_type(self, operator: str, left_type: DataType, right_type: DataType) -> DataType:
        """获取二元操作的结果类型"""
        if operator in ['+', '-', '*', '/']:
            if self._is_numeric_type(left_type) and self._is_numeric_type(right_type):
                return DataType.FLOAT if DataType.FLOAT in [left_type, right_type] else DataType.INT
        elif operator in ['=', '!=', '<', '>', '<=', '>=']:
            return DataType.BOOLEAN
        elif operator in ['AND', 'OR']:
            return DataType.BOOLEAN
        
        return DataType.UNKNOWN
    
    def _int_to_float(self, value):
        """整数转浮点数"""
        return float(value)
    
    def _float_to_int(self, value):
        """浮点数转整数"""
        return int(value)
    
    def _varchar_to_int(self, value):
        """字符串转整数"""
        try:
            return int(value)
        except ValueError:
            raise TypeError(f"Cannot convert '{value}' to integer")
    
    def _is_numeric_type(self, data_type: DataType) -> bool:
        """检查是否为数值类型"""
        return data_type in [DataType.INT, DataType.FLOAT, DataType.DECIMAL]
