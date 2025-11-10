# -*- coding: utf-8 -*-
"""
触发器异常处理器 - 负责处理触发器执行过程中的异常
"""
from typing import Dict, List, Optional, Any, Callable
import logging
from enum import Enum

logger = logging.getLogger(__name__)

class TriggerErrorType(Enum):
    """触发器错误类型"""
    SYNTAX_ERROR = "SYNTAX_ERROR"
    SEMANTIC_ERROR = "SEMANTIC_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    CONDITION_ERROR = "CONDITION_ERROR"
    PERMISSION_ERROR = "PERMISSION_ERROR"
    RESOURCE_ERROR = "RESOURCE_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"

class TriggerException(Exception):
    """触发器异常基类"""
    
    def __init__(self, message: str, error_type: TriggerErrorType = TriggerErrorType.UNKNOWN_ERROR, 
                 trigger_name: str = "", details: Optional[Dict] = None):
        super().__init__(message)
        self.message = message
        self.error_type = error_type
        self.trigger_name = trigger_name
        self.details = details or {}
    
    def __str__(self):
        return f"触发器异常 [{self.error_type.value}]: {self.message} (触发器: {self.trigger_name})"

class TriggerSyntaxError(TriggerException):
    """触发器语法错误"""
    
    def __init__(self, message: str, trigger_name: str = "", details: Optional[Dict] = None):
        super().__init__(message, TriggerErrorType.SYNTAX_ERROR, trigger_name, details)

class TriggerSemanticError(TriggerException):
    """触发器语义错误"""
    
    def __init__(self, message: str, trigger_name: str = "", details: Optional[Dict] = None):
        super().__init__(message, TriggerErrorType.SEMANTIC_ERROR, trigger_name, details)

class TriggerExecutionError(TriggerException):
    """触发器执行错误"""
    
    def __init__(self, message: str, trigger_name: str = "", details: Optional[Dict] = None):
        super().__init__(message, TriggerErrorType.EXECUTION_ERROR, trigger_name, details)

class TriggerConditionError(TriggerException):
    """触发器条件评估错误"""
    
    def __init__(self, message: str, trigger_name: str = "", details: Optional[Dict] = None):
        super().__init__(message, TriggerErrorType.CONDITION_ERROR, trigger_name, details)

class TriggerExceptionHandler:
    """触发器异常处理器"""
    
    def __init__(self):
        self.error_handlers: Dict[TriggerErrorType, Callable] = {
            TriggerErrorType.SYNTAX_ERROR: self._handle_syntax_error,
            TriggerErrorType.SEMANTIC_ERROR: self._handle_semantic_error,
            TriggerErrorType.EXECUTION_ERROR: self._handle_execution_error,
            TriggerErrorType.CONDITION_ERROR: self._handle_condition_error,
            TriggerErrorType.PERMISSION_ERROR: self._handle_permission_error,
            TriggerErrorType.RESOURCE_ERROR: self._handle_resource_error,
            TriggerErrorType.UNKNOWN_ERROR: self._handle_unknown_error
        }
        self.error_log: List[Dict[str, Any]] = []
    
    def handle_exception(self, exception: Exception, trigger_name: str = "", 
                        context: Optional[Dict] = None) -> Dict[str, Any]:
        """
        处理触发器异常
        
        Args:
            exception: 异常对象
            trigger_name: 触发器名称
            context: 执行上下文
            
        Returns:
            Dict[str, Any]: 处理结果
        """
        try:
            # 确定错误类型
            error_type = self._classify_exception(exception)
            
            # 创建触发器异常
            if isinstance(exception, TriggerException):
                trigger_exception = exception
            else:
                trigger_exception = TriggerException(
                    message=str(exception),
                    error_type=error_type,
                    trigger_name=trigger_name,
                    details={'original_exception': type(exception).__name__}
                )
            
            # 记录错误
            self._log_error(trigger_exception, context)
            
            # 调用相应的错误处理器
            handler = self.error_handlers.get(error_type, self._handle_unknown_error)
            result = handler(trigger_exception, context)
            
            return result
            
        except Exception as e:
            logger.error(f"处理触发器异常时发生错误: {e}")
            return {
                "success": False,
                "error": "异常处理失败",
                "message": str(e)
            }
    
    def _classify_exception(self, exception: Exception) -> TriggerErrorType:
        """分类异常类型"""
        if isinstance(exception, TriggerException):
            return exception.error_type
        elif isinstance(exception, SyntaxError):
            return TriggerErrorType.SYNTAX_ERROR
        elif isinstance(exception, (ValueError, TypeError, AttributeError)):
            return TriggerErrorType.SEMANTIC_ERROR
        elif isinstance(exception, (RuntimeError, OSError, IOError)):
            return TriggerErrorType.EXECUTION_ERROR
        elif isinstance(exception, (KeyError, IndexError)):
            return TriggerErrorType.CONDITION_ERROR
        elif isinstance(exception, PermissionError):
            return TriggerErrorType.PERMISSION_ERROR
        elif isinstance(exception, (MemoryError, OverflowError)):
            return TriggerErrorType.RESOURCE_ERROR
        else:
            return TriggerErrorType.UNKNOWN_ERROR
    
    def _log_error(self, exception: TriggerException, context: Optional[Dict] = None):
        """记录错误日志"""
        error_record = {
            "timestamp": self._get_current_timestamp(),
            "trigger_name": exception.trigger_name,
            "error_type": exception.error_type.value,
            "message": exception.message,
            "details": exception.details,
            "context": context or {}
        }
        self.error_log.append(error_record)
        
        # 记录到日志系统
        logger.error(f"触发器异常: {exception}")
    
    def _handle_syntax_error(self, exception: TriggerException, context: Optional[Dict] = None) -> Dict[str, Any]:
        """处理语法错误"""
        return {
            "success": False,
            "error": "语法错误",
            "message": f"触发器语法错误: {exception.message}",
            "suggestion": "请检查触发器语句的语法是否正确"
        }
    
    def _handle_semantic_error(self, exception: TriggerException, context: Optional[Dict] = None) -> Dict[str, Any]:
        """处理语义错误"""
        return {
            "success": False,
            "error": "语义错误",
            "message": f"触发器语义错误: {exception.message}",
            "suggestion": "请检查触发器引用的表、列是否存在，数据类型是否匹配"
        }
    
    def _handle_execution_error(self, exception: TriggerException, context: Optional[Dict] = None) -> Dict[str, Any]:
        """处理执行错误"""
        return {
            "success": False,
            "error": "执行错误",
            "message": f"触发器执行失败: {exception.message}",
            "suggestion": "请检查触发器主体中的SQL语句是否正确，相关表是否存在"
        }
    
    def _handle_condition_error(self, exception: TriggerException, context: Optional[Dict] = None) -> Dict[str, Any]:
        """处理条件评估错误"""
        return {
            "success": False,
            "error": "条件错误",
            "message": f"触发器条件评估失败: {exception.message}",
            "suggestion": "请检查WHEN条件中的表达式是否正确"
        }
    
    def _handle_permission_error(self, exception: TriggerException, context: Optional[Dict] = None) -> Dict[str, Any]:
        """处理权限错误"""
        return {
            "success": False,
            "error": "权限错误",
            "message": f"触发器权限不足: {exception.message}",
            "suggestion": "请检查是否有足够的权限执行触发器操作"
        }
    
    def _handle_resource_error(self, exception: TriggerException, context: Optional[Dict] = None) -> Dict[str, Any]:
        """处理资源错误"""
        return {
            "success": False,
            "error": "资源错误",
            "message": f"触发器资源不足: {exception.message}",
            "suggestion": "请检查系统资源是否充足"
        }
    
    def _handle_unknown_error(self, exception: TriggerException, context: Optional[Dict] = None) -> Dict[str, Any]:
        """处理未知错误"""
        return {
            "success": False,
            "error": "未知错误",
            "message": f"触发器发生未知错误: {exception.message}",
            "suggestion": "请联系系统管理员"
        }
    
    def get_error_log(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        获取错误日志
        
        Args:
            limit: 限制返回的记录数
            
        Returns:
            List[Dict[str, Any]]: 错误日志列表
        """
        if limit is None:
            return self.error_log.copy()
        else:
            return self.error_log[-limit:] if limit > 0 else []
    
    def clear_error_log(self):
        """清空错误日志"""
        self.error_log.clear()
    
    def get_error_statistics(self) -> Dict[str, Any]:
        """
        获取错误统计信息
        
        Returns:
            Dict[str, Any]: 错误统计信息
        """
        if not self.error_log:
            return {
                "total_errors": 0,
                "errors_by_type": {},
                "errors_by_trigger": {}
            }
        
        errors_by_type = {}
        errors_by_trigger = {}
        
        for error in self.error_log:
            # 按错误类型统计
            error_type = error["error_type"]
            errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1
            
            # 按触发器统计
            trigger_name = error["trigger_name"]
            if trigger_name:
                errors_by_trigger[trigger_name] = errors_by_trigger.get(trigger_name, 0) + 1
        
        return {
            "total_errors": len(self.error_log),
            "errors_by_type": errors_by_type,
            "errors_by_trigger": errors_by_trigger
        }
    
    def _get_current_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().isoformat()
