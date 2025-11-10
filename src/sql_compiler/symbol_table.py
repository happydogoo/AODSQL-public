# -*- coding: utf-8 -*-
"""
符号表实现 - 用于存储表结构元数据
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
from enum import Enum


class DataType(Enum):
    """数据类型枚举"""
    INT = "INT"
    INTEGER = "INTEGER"
    SMALLINT = "SMALLINT"
    BIGINT = "BIGINT"
    TINYINT = "TINYINT"
    FLOAT = "FLOAT"
    REAL = "REAL"
    DOUBLE = "DOUBLE"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    CHAR = "CHAR"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    BOOLEAN = "BOOLEAN"
    BLOB = "BLOB"
    UNKNOWN = "UNKNOWN"


@dataclass
class ColumnInfo:
    """列信息"""
    name: str
    data_type: DataType
    nullable: bool = True
    default_value: Optional[str] = None
    is_primary_key: bool = False
    
    def __str__(self):
        return f"{self.name}:{self.data_type.value}"


@dataclass
class TableInfo:
    """表信息"""
    name: str
    columns: List[ColumnInfo] = field(default_factory=list)
    created_at: Optional[str] = None
    
    def get_column(self, column_name: str) -> Optional[ColumnInfo]:
        """根据列名获取列信息"""
        for col in self.columns:
            if col.name.lower() == column_name.lower():
                return col
        return None
    
    def get_column_names(self) -> List[str]:
        """获取所有列名"""
        return [col.name for col in self.columns]
    
    def __str__(self):
        cols_str = ", ".join(str(col) for col in self.columns)
        return f"Table({self.name}): [{cols_str}]"


class SymbolTable:
    """符号表 - 管理数据库元数据"""
    
    def __init__(self):
        self.tables: Dict[str, TableInfo] = {}
        self.current_database: Optional[str] = None
    
    def add_table(self, table_info: TableInfo) -> None:
        """添加表到符号表"""
        table_name = table_info.name.lower()
        if table_name in self.tables:
            raise ValueError(f"Table '{table_info.name}' already exists")
        self.tables[table_name] = table_info
    
    def get_table(self, table_name: str) -> Optional[TableInfo]:
        """获取表信息"""
        # 处理TableReference对象
        if hasattr(table_name, 'table_name'):
            table_name = table_name.table_name
        return self.tables.get(table_name.lower())
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        # 处理TableReference对象
        if hasattr(table_name, 'table_name'):
            table_name = table_name.table_name
        return table_name.lower() in self.tables
    
    def column_exists(self, table_name: str, column_name: str) -> bool:
        """检查列是否存在"""
        table = self.get_table(table_name)
        if not table:
            return False
        return table.get_column(column_name) is not None
    
    def get_column_info(self, table_name: str, column_name: str) -> Optional[ColumnInfo]:
        """获取列信息"""
        table = self.get_table(table_name)
        if not table:
            return None
        return table.get_column(column_name)
    
    def get_all_table_names(self) -> List[str]:
        """获取所有表名"""
        return list(self.tables.keys())
    
    def remove_table(self, table_name: str) -> bool:
        """移除表"""
        if table_name in self.tables:
            del self.tables[table_name]
            return True
        return False
    
    def clear(self) -> None:
        """清空符号表"""
        self.tables.clear()
    
    def __str__(self):
        if not self.tables:
            return "SymbolTable: (empty)"
        
        result = "SymbolTable:\n"
        for table_name, table_info in self.tables.items():
            result += f"  {table_info}\n"
        return result


# 类型兼容性检查
class TypeChecker:
    """类型检查器"""
    
    # 数值类型集合
    NUMERIC_TYPES = {
        DataType.INT, DataType.INTEGER, DataType.SMALLINT, 
        DataType.BIGINT, DataType.TINYINT, DataType.FLOAT, 
        DataType.REAL, DataType.DOUBLE, DataType.DECIMAL, DataType.NUMERIC
    }
    
    # 字符串类型集合
    STRING_TYPES = {
        DataType.CHAR, DataType.VARCHAR, DataType.TEXT
    }
    
    # 日期时间类型集合
    DATETIME_TYPES = {
        DataType.DATE, DataType.TIME, DataType.TIMESTAMP
    }
    
    @classmethod
    def is_numeric_type(cls, data_type: DataType) -> bool:
        """检查是否为数值类型"""
        return data_type in cls.NUMERIC_TYPES
    
    @classmethod
    def is_string_type(cls, data_type: DataType) -> bool:
        """检查是否为字符串类型"""
        return data_type in cls.STRING_TYPES
    
    @classmethod
    def is_datetime_type(cls, data_type: DataType) -> bool:
        """检查是否为日期时间类型"""
        return data_type in cls.DATETIME_TYPES
    
    @classmethod
    def are_compatible(cls, type1: DataType, type2: DataType) -> bool:
        """检查两个类型是否兼容"""
        # 相同类型总是兼容
        if type1 == type2:
            return True
        
        # 数值类型之间兼容
        if cls.is_numeric_type(type1) and cls.is_numeric_type(type2):
            return True
        
        # 字符串类型之间兼容
        if cls.is_string_type(type1) and cls.is_string_type(type2):
            return True
        
        # 日期时间类型之间兼容
        if cls.is_datetime_type(type1) and cls.is_datetime_type(type2):
            return True
        
        # 字符串和日期时间类型兼容（简化处理）
        if (cls.is_string_type(type1) and cls.is_datetime_type(type2)) or \
           (cls.is_datetime_type(type1) and cls.is_string_type(type2)):
            return True
        
        return False
    
    @classmethod
    def get_literal_type(cls, value: str) -> Optional[DataType]:
        """根据字面量值推断类型"""
        # 尝试解析为数字
        try:
            if '.' in value:
                float(value)
                return DataType.FLOAT
            else:
                int(value)
                return DataType.INT
        except ValueError:
            pass
        
        # 检查是否为字符串
        if value.startswith("'") and value.endswith("'"):
            return DataType.VARCHAR
        
        # 检查是否为布尔值
        if value.upper() in ('TRUE', 'FALSE'):
            return DataType.BOOLEAN
        
        return None