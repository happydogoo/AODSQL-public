# -*- coding: utf-8 -*-
"""
增强的错误处理系统
"""
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum
import sys
import os

# 添加路径以便导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class ErrorSeverity(Enum):
    """错误严重程度"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    FATAL = "fatal"


class ErrorCategory(Enum):
    """错误类别"""
    LEXICAL = "lexical"
    SYNTAX = "syntax"
    SEMANTIC = "semantic"
    TYPE = "type"
    LOGIC = "logic"
    SYSTEM = "system"


@dataclass
class ErrorLocation:
    """错误位置信息"""
    line: int
    column: int
    length: int = 1
    
    def __str__(self):
        return f"line {self.line}, column {self.column}"


@dataclass
class CompilerError:
    """编译器错误"""
    message: str
    category: ErrorCategory
    severity: ErrorSeverity
    location: Optional[ErrorLocation] = None
    sql: str = ""
    suggestion: str = ""
    error_code: str = ""
    
    def __str__(self):
        result = f"{self.severity.value.upper()}: {self.message}"
        
        if self.location:
            result += f" at {self.location}"
        
        if self.suggestion:
            result += f"\nSuggestion: {self.suggestion}"
        
        if self.error_code:
            result += f"\nError Code: {self.error_code}"
        
        if self.sql and self.location:
            result += f"\n{self._highlight_error()}"
        
        return result
    
    def _highlight_error(self) -> str:
        """高亮显示错误位置"""
        if not self.sql or not self.location:
            return ""
        
        lines = self.sql.split('\n')
        if self.location.line <= len(lines):
            error_line = lines[self.location.line - 1]
            pointer = " " * (self.location.column - 1) + "^" * self.location.length
            return f"{error_line}\n{pointer}"
        return ""


class ErrorRegistry:
    """错误注册表"""
    
    def __init__(self):
        self.errors = {
            # 词法错误
            "LEX001": {
                "message": "Unknown token: '{token}'",
                "category": ErrorCategory.LEXICAL,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Check for typos or unsupported characters"
            },
            "LEX002": {
                "message": "Unterminated string literal",
                "category": ErrorCategory.LEXICAL,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Add closing quote for string literal"
            },
            "LEX003": {
                "message": "Invalid number format: '{number}'",
                "category": ErrorCategory.LEXICAL,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Check number format (e.g., 123.45, 1e10)"
            },
            
            # 语法错误
            "SYN001": {
                "message": "Expected '{expected}' but found '{found}'",
                "category": ErrorCategory.SYNTAX,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Check syntax rules for correct statement structure"
            },
            "SYN002": {
                "message": "Unexpected end of input",
                "category": ErrorCategory.SYNTAX,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Complete the SQL statement"
            },
            "SYN003": {
                "message": "Missing semicolon at end of statement",
                "category": ErrorCategory.SYNTAX,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Add semicolon (;) at the end of the statement"
            },
            
            # 语义错误
            "SEM001": {
                "message": "Table '{table}' does not exist",
                "category": ErrorCategory.SEMANTIC,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Check table name spelling or create the table first"
            },
            "SEM002": {
                "message": "Column '{column}' does not exist in table '{table}'",
                "category": ErrorCategory.SEMANTIC,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Check column name spelling or use correct column name"
            },
            "SEM003": {
                "message": "Table '{table}' already exists",
                "category": ErrorCategory.SEMANTIC,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Use different table name or DROP TABLE first"
            },
            
            # 类型错误
            "TYP001": {
                "message": "Type mismatch: cannot convert '{from_type}' to '{to_type}'",
                "category": ErrorCategory.TYPE,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Use explicit type conversion or correct the data types"
            },
            "TYP002": {
                "message": "Unknown data type: '{type}'",
                "category": ErrorCategory.TYPE,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Use supported data types: INT, FLOAT, VARCHAR, etc."
            },
            
            # 逻辑错误
            "LOG001": {
                "message": "Invalid aggregate function usage",
                "category": ErrorCategory.LOGIC,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Use aggregate functions only in SELECT with GROUP BY"
            },
            "LOG002": {
                "message": "Column '{column}' must appear in GROUP BY clause",
                "category": ErrorCategory.LOGIC,
                "severity": ErrorSeverity.ERROR,
                "suggestion": "Add column to GROUP BY or use aggregate function"
            }
        }
    
    def create_error(self, error_code: str, **kwargs) -> CompilerError:
        """创建错误对象"""
        if error_code not in self.errors:
            return CompilerError(
                message=f"Unknown error: {error_code}",
                category=ErrorCategory.SYSTEM,
                severity=ErrorSeverity.FATAL,
                error_code=error_code
            )
        
        error_info = self.errors[error_code]
        message = error_info["message"].format(**kwargs)
        
        # 只传递CompilerError支持的参数
        error_kwargs = {}
        if 'location' in kwargs:
            error_kwargs['location'] = kwargs['location']
        if 'sql' in kwargs:
            error_kwargs['sql'] = kwargs['sql']
        if 'suggestion' in kwargs:
            error_kwargs['suggestion'] = kwargs['suggestion']
        
        return CompilerError(
            message=message,
            category=error_info["category"],
            severity=error_info["severity"],
            suggestion=error_info.get("suggestion", ""),
            error_code=error_code,
            **error_kwargs
        )


class ErrorCollector:
    """错误收集器"""
    
    def __init__(self):
        self.errors: List[CompilerError] = []
        self.warnings: List[CompilerError] = []
        self.registry = ErrorRegistry()
    
    def add_error(self, error_code: str, location: Optional[ErrorLocation] = None, 
                  sql: str = "", **kwargs):
        """添加错误"""
        error = self.registry.create_error(error_code, **kwargs)
        error.location = location
        error.sql = sql
        self.errors.append(error)
    
    def add_warning(self, error_code: str, location: Optional[ErrorLocation] = None, 
                    sql: str = "", **kwargs):
        """添加警告"""
        warning = self.registry.create_error(error_code, **kwargs)
        warning.severity = ErrorSeverity.WARNING
        warning.location = location
        warning.sql = sql
        self.warnings.append(warning)
    
    def has_errors(self) -> bool:
        """检查是否有错误"""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """检查是否有警告"""
        return len(self.warnings) > 0
    
    def get_errors(self) -> List[CompilerError]:
        """获取所有错误"""
        return self.errors.copy()
    
    def get_warnings(self) -> List[CompilerError]:
        """获取所有警告"""
        return self.warnings.copy()
    
    def get_all_issues(self) -> List[CompilerError]:
        """获取所有问题和警告"""
        return self.errors + self.warnings
    
    def clear(self):
        """清空所有错误和警告"""
        self.errors.clear()
        self.warnings.clear()
    
    def format_report(self) -> str:
        """格式化错误报告"""
        if not self.has_errors() and not self.has_warnings():
            return "No errors or warnings found."
        
        report = []
        
        if self.has_errors():
            report.append("Errors:")
            for i, error in enumerate(self.errors, 1):
                report.append(f"  {i}. {error}")
        
        if self.has_warnings():
            report.append("Warnings:")
            for i, warning in enumerate(self.warnings, 1):
                report.append(f"  {i}. {warning}")
        
        return "\n".join(report)


class ErrorHandler:
    """错误处理器"""
    
    def __init__(self):
        self.collector = ErrorCollector()
        self.max_errors = 100  # 最大错误数量
    
    def handle_lexical_error(self, token: str, line: int, column: int, sql: str = ""):
        """处理词法错误"""
        location = ErrorLocation(line, column, len(token))
        self.collector.add_error(
            "LEX001", 
            location=location, 
            sql=sql, 
            token=token
        )
    
    def handle_syntax_error(self, expected: str, found: str, line: int, column: int, sql: str = ""):
        """处理语法错误"""
        location = ErrorLocation(line, column, len(found))
        self.collector.add_error(
            "SYN001", 
            location=location, 
            sql=sql, 
            expected=expected, 
            found=found
        )
    
    def handle_semantic_error(self, error_code: str, **kwargs):
        """处理语义错误"""
        self.collector.add_error(error_code, **kwargs)
    
    def handle_type_error(self, from_type: str, to_type: str, line: int = 0, column: int = 0, sql: str = ""):
        """处理类型错误"""
        location = ErrorLocation(line, column) if line > 0 else None
        self.collector.add_error(
            "TYP001", 
            location=location, 
            sql=sql, 
            from_type=from_type, 
            to_type=to_type
        )
    
    def should_continue(self) -> bool:
        """检查是否应该继续编译"""
        return len(self.collector.errors) < self.max_errors
    
    def get_report(self) -> str:
        """获取错误报告"""
        return self.collector.format_report()
    
    def clear(self):
        """清空错误"""
        self.collector.clear()


# 使用示例
def demonstrate_enhanced_error_handling():
    """演示增强的错误处理"""
    handler = ErrorHandler()
    
    # 模拟各种错误
    handler.handle_lexical_error("$invalid", 1, 10, "SELECT * FROM users WHERE id = $invalid;")
    handler.handle_syntax_error(";", "EOF", 1, 25, "SELECT * FROM users")
    handler.handle_semantic_error("SEM001", table="nonexistent_table")
    handler.handle_type_error("VARCHAR", "INT", 1, 15, "SELECT age + 'text' FROM users;")
    
    # 删除348,349行


if __name__ == "__main__":
    demonstrate_enhanced_error_handling()
