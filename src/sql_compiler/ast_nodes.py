# -*- coding: utf-8 -*-
"""
此文件定义了抽象语法树 (AST) 的所有节点类。
语法分析器 (Parser) 的输出就是一个由这些类的实例构成的树状结构。
每个类代表了 SQL 语言中的一个语法构造。
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Union

from .sql_token import Token


# --- 基础节点 ---

class ASTNode:
    """所有 AST 节点的基类，用于类型检查和未来的扩展。"""
    pass


# --- 表达式节点 (Expressions) ---
# 表达式是会产生一个值的代码片段，例如字面量、列名、二元运算等。

@dataclass
class Literal(ASTNode):
    """字面量节点，如数字 '123' 或字符串 'hello'。"""
    token: Token  # The token representing the literal (e.g., NUMBER, STRING)
    value: Union[int, float, str]

@dataclass
class Identifier(ASTNode):
    """标识符节点，如表名或列名。"""
    token: Token  # The IDENTIFIER token
    value: str

@dataclass
class BinaryExpr(ASTNode):
    """二元表达式节点，如 'age > 20'。"""
    left: 'Expression'        # 左操作数
    operator: Token           # 操作符 (e.g., =, >, <)
    right: 'Expression'       # 右操作数
    
    def __str__(self):
        """返回简洁的表达式字符串，如 'quantity * unit_price'"""
        left_str = str(self.left.value) if hasattr(self.left, 'value') else str(self.left)
        right_str = str(self.right.value) if hasattr(self.right, 'value') else str(self.right)
        op_str = self.operator[1] if isinstance(self.operator, tuple) else str(self.operator)
        return f"{left_str} {op_str} {right_str}"

@dataclass
class SubqueryExpression(ASTNode):
    """子查询表达式节点，如 '(SELECT id FROM table)'。"""
    subquery: 'SelectStatement'  # 子查询语句
    
    def __str__(self):
        return f"({self.subquery})"

@dataclass
class AggregateFunction(ASTNode):
    """聚合函数节点，如 COUNT(*), SUM(column), AVG(column) 等。"""
    function_name: str        # 函数名 (COUNT, SUM, AVG, MIN, MAX)
    argument: Optional['Expression'] = None  # 参数表达式，None表示COUNT(*)
    distinct: bool = False    # 是否使用DISTINCT
    alias: Optional[str] = None  # 别名

# --- 语句节点 (Statements) ---
# 语句是执行一个动作的完整指令。

@dataclass
class ColumnDefinition(ASTNode):
    """列定义节点，用于 CREATE TABLE 语句。"""
    column_name: Identifier
    data_type: Token  # The data type token (e.g., INT, VARCHAR)
    constraints: List[str] = field(default_factory=list)  # 列级约束

@dataclass
class ForeignKeyConstraint(ASTNode):
    """外键约束节点"""
    column_name: Identifier
    ref_table_name: Identifier
    ref_column_name: Identifier

@dataclass
class PrimaryKeyConstraint(ASTNode):
    """主键约束节点"""
    column_names: List[Identifier]

@dataclass
class UniqueConstraint(ASTNode):
    """唯一约束节点"""
    column_names: List[Identifier]

# --- 触发器相关节点 ---

@dataclass
class CreateTriggerStatement(ASTNode):
    """CREATE TRIGGER 语句节点"""
    trigger_name: Identifier
    table_name: Identifier
    timing: str  # BEFORE, AFTER, INSTEAD OF
    events: List[str]  # INSERT, UPDATE, DELETE
    is_row_level: bool  # FOR EACH ROW
    when_condition: Optional['Expression'] = None  # WHEN 条件
    trigger_body: List['Statement'] = field(default_factory=list)  # BEGIN...END 块中的语句

@dataclass
class DropTriggerStatement(ASTNode):
    """DROP TRIGGER 语句节点"""
    trigger_name: Identifier

@dataclass
class ShowTriggersStatement(ASTNode):
    """SHOW TRIGGERS 语句节点"""
    pass

@dataclass
class OldNewReference(ASTNode):
    """OLD/NEW 引用节点"""
    reference_type: str  # 'OLD' 或 'NEW'
    column_name: Identifier

@dataclass
class WhenCondition(ASTNode):
    """WHEN 条件节点"""
    condition: 'Expression'
    
    def encode(self, encoding='utf-8'):
        """提供encode方法以兼容序列化需求"""
        return str(self).encode(encoding)

@dataclass
class IfStatement(ASTNode):
    """IF 语句节点"""
    condition: 'Expression'
    then_statements: List['Statement']
    else_statements: List['Statement'] = field(default_factory=list)

@dataclass
class SignalStatement(ASTNode):
    """SIGNAL 语句节点"""
    sqlstate: str
    message: str

@dataclass
class CreateTableStatement(ASTNode):
    """CREATE TABLE 语句节点。"""
    table_name: Identifier
    columns: List[ColumnDefinition]
    constraints: List[Union[ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint]] = field(default_factory=list)

@dataclass
class InsertStatement(ASTNode):
    """INSERT INTO 语句节点。"""
    table_name: Identifier
    values: List[Literal]  # 简化版：只支持字面量列表

@dataclass
class SelectStatement(ASTNode):
    """SELECT 语句节点。"""
    select_list: List[Union[Identifier, Token, AggregateFunction]]  # 列名列表、'*' 或聚合函数
    from_table: Identifier
    joins: List['Join'] = field(default_factory=list)  # JOIN子句列表
    where_clause: Optional[BinaryExpr] = None

@dataclass
class UpdateStatement(ASTNode):
    """UPDATE 语句节点。"""
    table_name: Identifier
    set_clause: List[tuple]  # [(column, value), ...]
    where_clause: Optional[BinaryExpr] = None

@dataclass
class DeleteStatement(ASTNode):
    """DELETE 语句节点。"""
    table_name: Identifier
    where_clause: Optional[BinaryExpr] = None


@dataclass
class Join(ASTNode):
    """JOIN子句节点。"""
    join_type: str  # 'INNER', 'LEFT', 'RIGHT', 'FULL'
    right_table: 'TableReference'
    condition: 'JoinCondition'


@dataclass
class JoinCondition(ASTNode):
    """JOIN条件节点。"""
    left_table: str
    left_column: str
    operator: str
    right_table: str
    right_column: str


@dataclass
class TableReference(ASTNode):
    """表引用节点。"""
    table_name: str
    alias: Optional[str] = None

# --- 触发器相关节点 ---

@dataclass
class CreateTriggerStatement(ASTNode):
    """CREATE TRIGGER 语句节点。"""
    trigger_name: Identifier
    table_name: Identifier
    timing: str  # BEFORE, AFTER, INSTEAD OF
    events: List[str]  # INSERT, UPDATE, DELETE
    is_row_level: bool  # FOR EACH ROW
    when_condition: Optional['Expression'] = None
    trigger_body: List['Statement'] = field(default_factory=list)

@dataclass
class DropTriggerStatement(ASTNode):
    """DROP TRIGGER 语句节点。"""
    trigger_name: Identifier

@dataclass
class TriggerBody(ASTNode):
    """触发器主体节点。"""
    statements: List['Statement']

@dataclass
class OldNewReference(ASTNode):
    """OLD/NEW 引用节点，用于触发器中的OLD.column和NEW.column。"""
    reference_type: str  # OLD 或 NEW
    column_name: Identifier

@dataclass
class WhenCondition(ASTNode):
    """WHEN 条件节点。"""
    condition: 'Expression'

@dataclass
class IfStatement(ASTNode):
    """IF 语句节点。"""
    condition: 'Expression'
    then_statements: List['Statement']
    else_statements: Optional[List['Statement']] = None

@dataclass
class ShowTriggers(ASTNode):
    """SHOW TRIGGERS 语句节点。"""
    pass

# 类型定义 - 放在所有类定义之后
# Expression 类型可以是任何表达式节点的联合
Expression = Union[Literal, Identifier, BinaryExpr, AggregateFunction, OldNewReference]

@dataclass
class DeclareCursorStatement(ASTNode):
    """DECLARE CURSOR 语句节点"""
    cursor_name: Identifier
    query: 'SelectStatement'  # 游标的核心是它所关联的SELECT查询

@dataclass
class OpenCursorStatement(ASTNode):
    """OPEN CURSOR 语句节点"""
    cursor_name: Identifier

@dataclass
class FetchCursorStatement(ASTNode):
    """FETCH CURSOR 语句节点"""
    cursor_name: Identifier

@dataclass
class CloseCursorStatement(ASTNode):
    """CLOSE CURSOR 语句节点"""
    cursor_name: Identifier

# Statement 类型可以是任何语句节点的联合
Statement = Union[CreateTableStatement, InsertStatement, SelectStatement, CreateTriggerStatement, 
                 DropTriggerStatement, ShowTriggersStatement, IfStatement, UpdateStatement, DeleteStatement, 
                 ShowTriggers, SignalStatement, DeclareCursorStatement, OpenCursorStatement, 
                 FetchCursorStatement, CloseCursorStatement]
