# -*- coding: utf-8 -*-
"""
逻辑操作符定义 - 用于执行计划生成
"""
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from abc import ABC, abstractmethod


class OperatorType(Enum):
    """操作符类型枚举"""
    SCAN = "Scan"           # 表扫描
    INDEX_SCAN = "IndexScan" # 索引扫描
    FILTER = "Filter"       # 过滤
    PROJECT = "Project"     # 投影
    JOIN = "Join"          # 连接
    SORT = "Sort"          # 排序
    GROUP_BY = "GroupBy"    # 分组
    AGGREGATE = "Aggregate" # 聚合
    HAVING = "Having"       # HAVING子句
    LIMIT = "Limit"         # 限制
    INSERT = "Insert"      # 插入
    UPDATE = "Update"       # 更新
    DELETE = "Delete"      # 删除
    CREATE_INDEX = "CreateIndex" # 创建索引
    DROP_INDEX = "DropIndex"     # 删除索引
    CREATE_VIEW = "CreateView"   # 创建视图
    DROP_VIEW = "DropView"       # 删除视图
    ALTER_VIEW = "AlterView"     # 修改视图
    CREATE_TABLE = "CreateTable" # 创建表
    DROP_TABLE = "DropTable"     # 删除表
    CREATE_TRIGGER = "CreateTrigger" # 创建触发器
    DROP_TRIGGER = "DropTrigger"     # 删除触发器
    SHOW_TABLES = "ShowTables"       # 显示表
    SHOW_COLUMNS = "ShowColumns"     # 显示列
    SHOW_INDEX = "ShowIndex"         # 显示索引
    SHOW_TRIGGERS = "ShowTriggers"   # 显示触发器
    SHOW_VIEWS = "ShowViews"         # 显示视图
    EXPLAIN = "Explain"              # 解释查询
    DECLARE_CURSOR = "DeclareCursor" # 声明游标
    OPEN_CURSOR = "OpenCursor"       # 打开游标
    FETCH_CURSOR = "FetchCursor"     # 获取游标
    CLOSE_CURSOR = "CloseCursor"     # 关闭游标
    BEGIN_TRANSACTION = "BeginTransaction" # 开始事务
    COMMIT_TRANSACTION = "CommitTransaction" # 提交事务
    ROLLBACK_TRANSACTION = "RollbackTransaction" # 回滚事务


class JoinType(Enum):
    """连接类型枚举"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"
    CARTESIAN = "CARTESIAN"


class JoinMethod(Enum):
    """连接方法枚举"""
    NESTED_LOOP = "nested_loop"    # 嵌套循环连接
    HASH_JOIN = "hash_join"        # 哈希连接
    SORT_MERGE = "sort_merge"      # 排序合并连接


@dataclass
class ColumnReference:
    """列引用"""
    table_name: Optional[str]
    column_name: str
    alias: Optional[str] = None
    
    def __init__(self, table_name: Optional[str], column_name: str, alias: Optional[str] = None):
        self.table_name = table_name
        self.column_name = column_name
        self.alias = alias
    
    def __str__(self):
        if self.table_name:
            base_str = f"{self.table_name}.{self.column_name}"
        else:
            base_str = self.column_name
        
        # 如果有别名，添加到字符串中
        if self.alias:
            return f"{base_str} AS {self.alias}"
        else:
            return base_str


@dataclass
class Expression:
    """表达式基类"""
    pass


@dataclass
class LiteralExpression(Expression):
    """字面量表达式"""
    value: Any
    data_type: Optional[str] = None
    
    def __str__(self):
        return str(self.value)


@dataclass
class ColumnExpression(Expression):
    """列表达式"""
    column_ref: ColumnReference
    
    def __str__(self):
        return str(self.column_ref)


@dataclass
class BinaryExpression(Expression):
    """二元表达式"""
    left: Expression
    operator: str
    right: Expression
    
    def __str__(self):
        return f"({self.left} {self.operator} {self.right})"


@dataclass
class BetweenExpression(Expression):
    """BETWEEN表达式"""
    left: Expression
    start: Expression
    end: Expression
    negated: bool = False
    
    def __str__(self):
        op = "NOT BETWEEN" if self.negated else "BETWEEN"
        return f"({self.left} {op} {self.start} AND {self.end})"


@dataclass
class InExpression(Expression):
    """IN值列表表达式"""
    left: Expression
    values: List[Expression]
    operator: str = "IN"
    
    def __str__(self):
        values_str = "(" + ", ".join(str(v) for v in self.values) + ")"
        return f"({self.left} {self.operator} {values_str})"


@dataclass
class InSubqueryExpression(Expression):
    """IN子查询表达式"""
    left: Expression
    subquery: Expression
    operator: str = "IN"
    
    def __str__(self):
        return f"({self.left} {self.operator} ({self.subquery}))"


@dataclass
class SubqueryExpression(Expression):
    """子查询表达式"""
    subquery_plan: 'LogicalPlan'
    
    def __str__(self):
        return f"({self.subquery_plan})"


@dataclass
class AggregateExpression(Expression):
    """聚合函数表达式"""
    function_name: str
    argument: Optional[Expression] = None
    distinct: bool = False
    alias: Optional[str] = None
    
    def __str__(self):
        if self.argument:
            arg_str = str(self.argument)
        else:
            arg_str = "*"
        
        distinct_str = "DISTINCT " if self.distinct else ""
        alias_str = f" AS {self.alias}" if self.alias else ""
        
        return f"{self.function_name}({distinct_str}{arg_str}){alias_str}"


class LogicalOperator(ABC):
    """逻辑操作符基类"""
    
    def __init__(self, operator_type: OperatorType):
        self.operator_type = operator_type
        self.children: List['LogicalOperator'] = []
        self.parent: Optional['LogicalOperator'] = None
    
    def add_child(self, child: 'LogicalOperator'):
        """添加子操作符"""
        child.parent = self
        self.children.append(child)
    
    def get_children(self) -> List['LogicalOperator']:
        """获取子操作符列表"""
        return self.children.copy()
    
    @abstractmethod
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        pass
    
    def __str__(self):
        return f"{self.operator_type.value}"


class ScanOperator(LogicalOperator):
    """表扫描操作符"""
    
    def __init__(self, table_name: str, alias: Optional[str] = None):
        super().__init__(OperatorType.SCAN)
        self.table_name = table_name
        self.alias = alias or table_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SCAN",
            "properties": {
                "table_name": self.table_name,
                "alias": self.alias
            },
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        return f"Scan({self.table_name})"


class IndexScanOperator(LogicalOperator):
    """索引扫描操作符"""
    
    def __init__(self, table_name: str, index_name: str, column_name: str, predicate: Optional[str] = None):
        super().__init__(OperatorType.INDEX_SCAN)
        self.table_name = table_name
        self.index_name = index_name
        self.column_name = column_name
        self.predicate = predicate  # 简化为字符串
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "INDEX_SCAN",
            "table_name": self.table_name,
            "index_name": self.index_name,
            "column_name": self.column_name,
            "predicate": self.predicate,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        pred = f", predicate={self.predicate}" if self.predicate else ""
        return f"IndexScan({self.table_name}, {self.index_name} on {self.column_name}{pred})"


class CreateIndexOperator(LogicalOperator):
    """创建索引操作符（逻辑DDL）"""
    def __init__(self, index_name: str, table_name: str, column_or_columns):
        super().__init__(OperatorType.CREATE_INDEX)
        self.index_name = index_name
        self.table_name = table_name
        # 支持单列或多列
        if isinstance(column_or_columns, list):
            self.columns = column_or_columns
            self.column_name = column_or_columns[0]  # 保持向后兼容性
        else:
            self.column_name = column_or_columns
            self.columns = [column_or_columns]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CREATE_INDEX",
            "index_name": self.index_name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "columns": self.columns,
            "children": []
        }


class DropIndexOperator(LogicalOperator):
    """删除索引操作符（逻辑DDL）"""
    def __init__(self, index_name: str):
        super().__init__(OperatorType.DROP_INDEX)
        self.index_name = index_name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DROP_INDEX",
            "index_name": self.index_name,
            "children": []
        }

    def __str__(self):
        if len(self.columns) == 1:
            return f"CreateIndex({self.index_name} ON {self.table_name}({self.column_name}))"
        else:
            return f"CreateIndex({self.index_name} ON {self.table_name}({', '.join(self.columns)}))"


class FilterOperator(LogicalOperator):
    """过滤操作符"""
    
    def __init__(self, condition: Expression):
        super().__init__(OperatorType.FILTER)
        self.condition = condition
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "FILTER",
            "condition": str(self.condition),
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        return f"Filter({self.condition})"


class ProjectOperator(LogicalOperator):
    """投影操作符"""
    
    def __init__(self, columns: List[Union[str, ColumnReference]]):
        super().__init__(OperatorType.PROJECT)
        self.columns = columns
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "PROJECT",
            "columns": [str(col) for col in self.columns],
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        cols_str = ", ".join(str(col) for col in self.columns)
        return f"Project([{cols_str}])"


class JoinOperator(LogicalOperator):
    """连接操作符"""
    
    def __init__(self, join_type: JoinType, condition: Expression, join_method: JoinMethod = JoinMethod.NESTED_LOOP):
        super().__init__(OperatorType.JOIN)
        self.join_type = join_type
        self.condition = condition
        self.join_method = join_method
        self.left_child = None
        self.right_child = None
        # children属性已经在super().__init__()中初始化了
        self._children_set = False  # 跟踪children是否被设置过
    
    def to_dict(self) -> Dict[str, Any]:
        # 如果children为空但left_child和right_child存在，重新设置children
        if not self.children and self.left_child and self.right_child:
            self.children = [self.left_child, self.right_child]
        
        children_dicts = []
        for child in self.children:
            try:
                child_dict = child.to_dict()
                children_dicts.append(child_dict)
            except Exception as e:
                children_dicts.append({"type": "ERROR", "error": str(e)})
        
        return {
            "type": "JOIN",
            "properties": {
                "join_type": self.join_type.value,
                "join_method": self.join_method.value,
                "condition": str(self.condition) if self.condition else None
            },
            "children": children_dicts
        }
    
    def __str__(self):
        return f"Join({self.join_type.value}, {self.join_method.value}, {self.condition})"


class SortOperator(LogicalOperator):
    """排序操作符"""
    
    def __init__(self, order_by: List[ColumnReference], ascending: List[bool]):
        super().__init__(OperatorType.SORT)
        self.order_by = order_by
        self.ascending = ascending
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SORT",
            "order_by": [str(col) for col in self.order_by],
            "ascending": self.ascending,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        order_str = ", ".join(f"{col} {'ASC' if asc else 'DESC'}" 
                             for col, asc in zip(self.order_by, self.ascending))
        return f"Sort([{order_str}])"


class OrderByOperator(LogicalOperator):
    """ORDER BY操作符"""
    
    def __init__(self, order_items: List[Dict[str, Any]]):
        super().__init__(OperatorType.SORT)
        self.order_items = order_items
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ORDER_BY",
            "order_items": self.order_items,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        order_str = ", ".join(f"{item['column']} {item['direction']}" for item in self.order_items)
        return f"OrderBy([{order_str}])"


class LimitOperator(LogicalOperator):
    """限制操作符"""
    
    def __init__(self, limit: int, offset: int = 0):
        super().__init__(OperatorType.LIMIT)
        self.limit = limit
        self.offset = offset
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "LIMIT",
            "limit": self.limit,
            "offset": self.offset,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        if self.offset > 0:
            return f"Limit({self.limit}, offset={self.offset})"
        return f"Limit({self.limit})"


class InsertOperator(LogicalOperator):
    """插入操作符"""
    
    def __init__(self, table_name: str, values: List[Any]):
        super().__init__(OperatorType.INSERT)
        self.table_name = table_name
        self.values = values
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "INSERT",
            "table_name": self.table_name,
            "values": self.values,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        values_str = ", ".join(str(v) for v in self.values)
        return f"Insert({self.table_name}, [{values_str}])"


class UpdateOperator(LogicalOperator):
    """更新操作符"""
    
    def __init__(self, table_name: str, set_clause: Dict[str, Any], where_clause: Optional[Expression] = None):
        super().__init__(OperatorType.UPDATE)
        self.table_name = table_name
        self.set_clause = set_clause
        self.where_clause = where_clause
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "UPDATE",
            "table_name": self.table_name,
            "set_clause": self.set_clause,
            "where_clause": str(self.where_clause) if self.where_clause else None,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        set_str = ", ".join(f"{k}={v}" for k, v in self.set_clause.items())
        where_str = f" WHERE {self.where_clause}" if self.where_clause else ""
        return f"Update({self.table_name}, SET {set_str}{where_str})"


class DeleteOperator(LogicalOperator):
    """删除操作符"""
    
    def __init__(self, table_name: str, where_clause: Optional[Expression] = None):
        super().__init__(OperatorType.DELETE)
        self.table_name = table_name
        self.where_clause = where_clause
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DELETE",
            "table_name": self.table_name,
            "where_clause": str(self.where_clause) if self.where_clause else None,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        where_str = f" WHERE {self.where_clause}" if self.where_clause else ""
        return f"Delete({self.table_name}{where_str})"


class LogicalPlan:
    """逻辑执行计划"""
    
    def __init__(self, root: LogicalOperator):
        self.root = root
        # 添加node_type属性以兼容优化器代码
        if hasattr(root, 'operator_type'):
            self.node_type = root.operator_type.value
        else:
            self.node_type = type(root).__name__
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            "plan_type": "logical",
            "root": self.root.to_dict()
        }
    
    def print_plan(self, indent: int = 0):
        """打印执行计划"""
        prefix = "  " * indent
        print(f"{prefix}{self.root}")
        
        for child in self.root.get_children():
            child_plan = LogicalPlan(child)
            child_plan.print_plan(indent + 1)
    
    def __str__(self):
        return f"LogicalPlan({self.root})"


class CreateViewOperator(LogicalOperator):
    """创建视图操作符"""
    
    def __init__(self, view_name: str, definition: str, schema_name: str = 'public', 
                 creator: str = 'system', is_updatable: bool = False):
        super().__init__(OperatorType.CREATE_VIEW)
        self.view_name = view_name
        self.definition = definition
        self.schema_name = schema_name
        self.creator = creator
        self.is_updatable = is_updatable
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CREATE_VIEW",
            "view_name": self.view_name,
            "definition": self.definition,
            "schema_name": self.schema_name,
            "creator": self.creator,
            "is_updatable": self.is_updatable,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        return f"CreateView({self.view_name}, {self.definition})"


class DropViewOperator(LogicalOperator):
    """删除视图操作符"""
    
    def __init__(self, view_name: str):
        super().__init__(OperatorType.DROP_VIEW)
        self.view_name = view_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DROP_VIEW",
            "view_name": self.view_name,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        return f"DropView({self.view_name})"


class AlterViewOperator(LogicalOperator):
    """修改视图操作符"""
    
    def __init__(self, view_name: str, definition: str, is_updatable: Optional[bool] = None):
        super().__init__(OperatorType.ALTER_VIEW)
        self.view_name = view_name
        self.definition = definition
        self.is_updatable = is_updatable
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "ALTER_VIEW",
            "view_name": self.view_name,
            "definition": self.definition,
            "is_updatable": self.is_updatable,
            "children": [child.to_dict() for child in self.children]
        }
    
    def __str__(self):
        return f"AlterView({self.view_name}, {self.definition})"


class CreateTableOperator(LogicalOperator):
    """创建表操作符"""
    def __init__(self, table_name: str, columns: list):
        super().__init__(OperatorType.CREATE_TABLE)
        self.table_name = table_name
        self.columns = columns
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CREATE_TABLE",
            "table_name": self.table_name,
            "columns": self.columns
        }
    
    def __str__(self):
        return f"CreateTable({self.table_name})"


class DropTableOperator(LogicalOperator):
    """删除表操作符"""
    def __init__(self, table_name: str, if_exists: bool = False):
        super().__init__(OperatorType.DROP_TABLE)
        self.table_name = table_name
        self.if_exists = if_exists
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DROP_TABLE",
            "table_name": self.table_name,
            "if_exists": self.if_exists
        }
    
    def __str__(self):
        return f"DropTable({self.table_name}, if_exists={self.if_exists})"


class CreateTriggerOperator(LogicalOperator):
    """创建触发器操作符"""
    def __init__(self, trigger_name: str, table_name: str, timing: str, events: list, 
                 is_row_level: bool, when_condition: Any, trigger_body: list):
        super().__init__(OperatorType.CREATE_TRIGGER)
        self.trigger_name = trigger_name
        self.table_name = table_name
        self.timing = timing
        self.events = events
        self.is_row_level = is_row_level
        self.when_condition = when_condition
        self.trigger_body = trigger_body
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CREATE_TRIGGER",
            "trigger_name": self.trigger_name,
            "table_name": self.table_name,
            "timing": self.timing,
            "events": self.events,
            "is_row_level": self.is_row_level,
            "when_condition": self.when_condition,
            "trigger_body": self.trigger_body
        }
    
    def __str__(self):
        return f"CreateTrigger({self.trigger_name} ON {self.table_name})"


class DropTriggerOperator(LogicalOperator):
    """删除触发器操作符"""
    def __init__(self, trigger_name: str):
        super().__init__(OperatorType.DROP_TRIGGER)
        self.trigger_name = trigger_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DROP_TRIGGER",
            "trigger_name": self.trigger_name
        }
    
    def __str__(self):
        return f"DropTrigger({self.trigger_name})"


class ShowTablesOperator(LogicalOperator):
    """显示表操作符"""
    def __init__(self):
        super().__init__(OperatorType.SHOW_TABLES)
    
    def to_dict(self) -> Dict[str, Any]:
        return {"type": "SHOW_TABLES"}
    
    def __str__(self):
        return "ShowTables()"


class ShowViewsOperator(LogicalOperator):
    """显示视图操作符"""
    def __init__(self):
        super().__init__(OperatorType.SHOW_VIEWS)
    
    def to_dict(self) -> Dict[str, Any]:
        return {"type": "SHOW_VIEWS"}
    
    def __str__(self):
        return "ShowViews()"


class ShowColumnsOperator(LogicalOperator):
    """显示列操作符"""
    def __init__(self, table_name: str):
        super().__init__(OperatorType.SHOW_COLUMNS)
        self.table_name = table_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SHOW_COLUMNS",
            "table_name": self.table_name
        }
    
    def __str__(self):
        return f"ShowColumns({self.table_name})"


class ShowIndexOperator(LogicalOperator):
    """显示索引操作符"""
    def __init__(self, table_name: str):
        super().__init__(OperatorType.SHOW_INDEX)
        self.table_name = table_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "SHOW_INDEX",
            "table_name": self.table_name
        }
    
    def __str__(self):
        return f"ShowIndex({self.table_name})"


class ShowTriggersOperator(LogicalOperator):
    """显示触发器操作符"""
    def __init__(self):
        super().__init__(OperatorType.SHOW_TRIGGERS)
    
    def to_dict(self) -> Dict[str, Any]:
        return {"type": "SHOW_TRIGGERS"}
    
    def __str__(self):
        return "ShowTriggers()"


class ExplainOperator(LogicalOperator):
    """解释查询操作符"""
    def __init__(self, query):
        super().__init__(OperatorType.EXPLAIN)
        self.query = query
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "EXPLAIN",
            "query": str(self.query)
        }
    
    def __str__(self):
        return f"Explain({self.query})"


class GroupByOperator(LogicalOperator):
    """分组操作符"""
    def __init__(self, group_columns):
        super().__init__(OperatorType.GROUP_BY)
        self.group_columns = group_columns
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "GROUP_BY",
            "group_columns": self.group_columns
        }
    
    def __str__(self):
        return f"GroupBy({self.group_columns})"


class HavingOperator(LogicalOperator):
    """HAVING子句操作符"""
    def __init__(self, condition):
        super().__init__(OperatorType.HAVING)
        self.condition = condition
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "HAVING",
            "condition": str(self.condition)
        }
    
    def __str__(self):
        return f"Having({self.condition})"


# --- 游标相关逻辑操作符 ---
class DeclareCursorOperator(LogicalOperator):
    """声明游标操作符"""
    def __init__(self, cursor_name: str, query_plan: 'LogicalOperator'):
        super().__init__(OperatorType.DECLARE_CURSOR)
        self.cursor_name = cursor_name
        self.query_plan = query_plan
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "DECLARE_CURSOR",
            "cursor_name": self.cursor_name,
            "query_plan": self.query_plan.to_dict() if self.query_plan else None
        }
    
    def __str__(self):
        return f"DeclareCursor({self.cursor_name})"


class OpenCursorOperator(LogicalOperator):
    """打开游标操作符"""
    def __init__(self, cursor_name: str):
        super().__init__(OperatorType.OPEN_CURSOR)
        self.cursor_name = cursor_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "OPEN_CURSOR",
            "cursor_name": self.cursor_name
        }
    
    def __str__(self):
        return f"OpenCursor({self.cursor_name})"


class FetchCursorOperator(LogicalOperator):
    """获取游标操作符"""
    def __init__(self, cursor_name: str):
        super().__init__(OperatorType.FETCH_CURSOR)
        self.cursor_name = cursor_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "FETCH_CURSOR",
            "cursor_name": self.cursor_name
        }
    
    def __str__(self):
        return f"FetchCursor({self.cursor_name})"


class CloseCursorOperator(LogicalOperator):
    """关闭游标操作符"""
    def __init__(self, cursor_name: str):
        super().__init__(OperatorType.CLOSE_CURSOR)
        self.cursor_name = cursor_name
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "CLOSE_CURSOR",
            "cursor_name": self.cursor_name
        }
    
    def __str__(self):
        return f"CloseCursor({self.cursor_name})"


class AggregateOperator(LogicalOperator):
    """聚合操作符"""
    def __init__(self, columns, table_name, child, group_by_columns=None, having_clause=None):
        super().__init__(OperatorType.AGGREGATE)
        self.columns = columns
        self.table_name = table_name
        self.child = child
        self.group_by_columns = group_by_columns
        self.having_clause = having_clause
        
        # 添加子节点到children列表
        if child:
            self.add_child(child)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "type": "AGGREGATE",
            "columns": self.columns,
            "table_name": self.table_name,
            "group_by_columns": self.group_by_columns,
            "having_clause": str(self.having_clause) if self.having_clause else None
        }
        
        # 添加子节点信息
        if self.child:
            result["children"] = [self.child.to_dict()]
        
        return result
    
    def __str__(self):
        return f"Aggregate({self.columns})"


class BeginTransactionOperator(LogicalOperator):
    """开始事务操作符"""
    
    def __init__(self):
        super().__init__(OperatorType.BEGIN_TRANSACTION)
    
    def to_dict(self):
        return {
            "type": "BEGIN_TRANSACTION",
            "properties": {
                "operator_type": self.operator_type.value
            }
        }
    
    def __str__(self):
        return "BeginTransaction()"


class CommitTransactionOperator(LogicalOperator):
    """提交事务操作符"""
    
    def __init__(self):
        super().__init__(OperatorType.COMMIT_TRANSACTION)
    
    def to_dict(self):
        return {
            "type": "COMMIT_TRANSACTION",
            "properties": {
                "operator_type": self.operator_type.value
            }
        }
    
    def __str__(self):
        return "CommitTransaction()"


class RollbackTransactionOperator(LogicalOperator):
    """回滚事务操作符"""
    
    def __init__(self):
        super().__init__(OperatorType.ROLLBACK_TRANSACTION)
    
    def to_dict(self):
        return {
            "type": "ROLLBACK_TRANSACTION",
            "properties": {
                "operator_type": self.operator_type.value
            }
        }
    
    def __str__(self):
        return "RollbackTransaction()"