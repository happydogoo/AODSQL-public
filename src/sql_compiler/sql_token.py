# -*- coding: utf-8 -*-
"""
此文件定义了 SQL 词法分析所需的所有 Token 结构。
它包含了 Token 类型枚举 (TokenType) 和 Token 类本身。
"""
import enum


class TokenType(enum.Enum):
    """
    TokenType 枚举定义了所有可能的 Token 类型。
    这有助于词法分析器和语法分析器识别和处理不同的语言元素。
    """
    # --- 单字符 Token ---
    LPAREN = "("          # 左括号
    RPAREN = ")"          # 右括号
    COMMA = ","           # 逗号
    SEMICOLON = ";"       # 分号
    ASTERISK = "*"        # 星号
    EQUAL = "="           # 等号
    GT = ">"              # 大于号
    LT = "<"              # 小于号

    # --- 多字符 Token ---
    # 可以在这里添加如 !=, >=, <= 等

    # --- 字面量 (Literals) ---
    IDENTIFIER = "IDENTIFIER"  # 标识符，如表名、列名
    NUMBER = "NUMBER"          # 数字常量
    STRING = "STRING"          # 字符串常量

    # --- SQL 关键字 (Keywords) ---
    # DDL (数据定义语言)
    CREATE = "CREATE"
    DROP = "DROP"
    TABLE = "TABLE"
    TRIGGER = "TRIGGER"
    EXISTS = "EXISTS"
    # ALTER = "ALTER"       # 可选扩展

    # DML (数据操作语言)
    INSERT = "INSERT"
    INTO = "INTO"
    VALUES = "VALUES"
    SELECT = "SELECT"
    FROM = "FROM"
    WHERE = "WHERE"
    ON = "ON"
    # UPDATE = "UPDATE"     # 可选扩展
    DELETE = "DELETE"     # 支持DELETE语句

    # 数据类型
    INT = "INT"
    VARCHAR = "VARCHAR"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    
    # 触发器关键字
    BEFORE = "BEFORE"
    AFTER = "AFTER"
    INSTEAD = "INSTEAD"
    OF = "OF"
    FOR = "FOR"
    EACH = "EACH"
    ROW = "ROW"
    WHEN = "WHEN"
    BEGIN = "BEGIN"
    END = "END"
    IF = "IF"
    THEN = "THEN"
    ELSE = "ELSE"
    NEW = "NEW"
    OLD = "OLD"
    SIGNAL = "SIGNAL"
    SQLSTATE = "SQLSTATE"

    # --- 特殊 Token ---
    EOF = "EOF"            # 文件结束符 (End of File)
    ILLEGAL = "ILLEGAL"    # 非法字符


class Token:
    """
    Token 类代表一个从源代码中识别出的词法单元。
    它包含了类型、字面值以及在源文件中的位置信息，便于后续处理和错误定位。
    """
    def __init__(self, token_type: TokenType, literal: str, line: int = 0, col: int = 0):
        """
        初始化一个 Token 对象。

        :param token_type: Token 的类型 (来自 TokenType 枚举)。
        :param literal: Token 的字面值 (e.g., "SELECT", "my_table", "123")。
        :param line: Token 在源文件中的起始行号。
        :param col: Token 在源文件中的起始列号。
        """
        self.type = token_type
        self.literal = literal
        self.line = line
        self.col = col

    def __str__(self) -> str:
        """返回 Token 的字符串表示，方便打印和调试。"""
        type_name = self.type.name if hasattr(self.type, 'name') else str(self.type)
        return f"Token[Type: {type_name}, Literal: '{self.literal}', Pos: {self.line}:{self.col}]"

    def __repr__(self) -> str:
        """返回 Token 的官方表示，通常与 __str__ 相同。"""
        return self.__str__()


# 预留关键字字典，用于在词法分析阶段快速区分标识符和关键字。
# Lexer 在解析出一个标识符后，应查询此字典。如果存在，则其 TokenType 为对应的关键字类型，
# 否则，其 TokenType 为 IDENTIFIER。
RESERVED_KEYWORDS = {
    # DDL
    "CREATE": TokenType.CREATE,
    "DROP": TokenType.DROP,
    "TABLE": TokenType.TABLE,
    "TRIGGER": TokenType.TRIGGER,
    "EXISTS": TokenType.EXISTS,
    # DML
    "INSERT": TokenType.INSERT,
    "INTO": TokenType.INTO,
    "VALUES": TokenType.VALUES,
    "SELECT": TokenType.SELECT,
    "FROM": TokenType.FROM,
    "WHERE": TokenType.WHERE,
    "ON": TokenType.ON,
    "DELETE": TokenType.DELETE,
    # Data Types
    "INT": TokenType.INT,
    "VARCHAR": TokenType.VARCHAR,
    "DECIMAL": TokenType.DECIMAL,
    "DATE": TokenType.DATE,
    # 触发器关键字
    "BEFORE": TokenType.BEFORE,
    "AFTER": TokenType.AFTER,
    "INSTEAD": TokenType.INSTEAD,
    "OF": TokenType.OF,
    "FOR": TokenType.FOR,
    "EACH": TokenType.EACH,
    "ROW": TokenType.ROW,
    "WHEN": TokenType.WHEN,
    "BEGIN": TokenType.BEGIN,
    "END": TokenType.END,
    "IF": TokenType.IF,
    "THEN": TokenType.THEN,
    "ELSE": TokenType.ELSE,
    "NEW": TokenType.NEW,
    "OLD": TokenType.OLD,
    "SIGNAL": TokenType.SIGNAL,
    "SQLSTATE": TokenType.SQLSTATE,
}