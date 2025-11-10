# -*- coding: utf-8 -*-
"""
触发器管理器 - 负责存储、管理和执行触发器
"""
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
from .trigger_condition_evaluator import TriggerConditionEvaluator
from .trigger_exception_handler import TriggerExceptionHandler, TriggerException

# 设置日志
logger = logging.getLogger(__name__)

class TriggerTiming(Enum):
    """触发器触发时机"""
    BEFORE = "BEFORE"
    AFTER = "AFTER"
    INSTEAD_OF = "INSTEAD OF"

class TriggerEvent(Enum):
    """触发器事件类型"""
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

@dataclass
class TriggerInfo:
    """触发器信息"""
    name: str
    table_name: str
    timing: TriggerTiming
    events: List[TriggerEvent]
    is_row_level: bool
    when_condition: Optional[Any] = None  # 条件表达式AST节点
    trigger_body: List[Any] = None  # 触发器主体语句列表
    created_at: str = ""
    
    def __post_init__(self):
        if self.trigger_body is None:
            self.trigger_body = []

class TriggerManager:
    """触发器管理器"""
    
    def __init__(self):
        # 存储触发器信息，按表名分组
        self._triggers_by_table: Dict[str, List[TriggerInfo]] = {}
        # 存储所有触发器，按名称索引
        self._triggers_by_name: Dict[str, TriggerInfo] = {}
        # 触发器执行历史（用于调试）
        self._execution_history: List[Dict[str, Any]] = []
        # 条件评估器
        self._condition_evaluator = TriggerConditionEvaluator()
        # 异常处理器
        self._exception_handler = TriggerExceptionHandler()
    
    def create_trigger(self, trigger_info: TriggerInfo) -> bool:
        """
        创建触发器
        
        Args:
            trigger_info: 触发器信息
            
        Returns:
            bool: 创建是否成功
        """
        try:
            # 检查触发器名称是否已存在
            if trigger_info.name in self._triggers_by_name:
                logger.warning(f"触发器 {trigger_info.name} 已存在")
                return False
            
            # 添加到按表名分组的字典
            if trigger_info.table_name not in self._triggers_by_table:
                self._triggers_by_table[trigger_info.table_name] = []
            
            self._triggers_by_table[trigger_info.table_name].append(trigger_info)
            self._triggers_by_name[trigger_info.name] = trigger_info
            
            logger.info(f"成功创建触发器: {trigger_info.name}")
            return True
            
        except Exception as e:
            logger.error(f"创建触发器失败: {e}")
            return False
    
    def drop_trigger(self, trigger_name: str) -> bool:
        """
        删除触发器
        
        Args:
            trigger_name: 触发器名称
            
        Returns:
            bool: 删除是否成功
        """
        try:
            if trigger_name not in self._triggers_by_name:
                logger.warning(f"触发器 {trigger_name} 不存在")
                return False
            
            trigger_info = self._triggers_by_name[trigger_name]
            table_name = trigger_info.table_name
            
            # 从按表名分组的字典中删除
            if table_name in self._triggers_by_table:
                self._triggers_by_table[table_name] = [
                    t for t in self._triggers_by_table[table_name] 
                    if t.name != trigger_name
                ]
                # 如果表下没有触发器了，删除表条目
                if not self._triggers_by_table[table_name]:
                    del self._triggers_by_table[table_name]
            
            # 从按名称索引的字典中删除
            del self._triggers_by_name[trigger_name]
            
            logger.info(f"成功删除触发器: {trigger_name}")
            return True
            
        except Exception as e:
            logger.error(f"删除触发器失败: {e}")
            return False
    
    def get_triggers_for_table(self, table_name: str) -> List[TriggerInfo]:
        """
        获取指定表的所有触发器
        
        Args:
            table_name: 表名
            
        Returns:
            List[TriggerInfo]: 触发器列表
        """
        return self._triggers_by_table.get(table_name, [])
    
    def get_triggers_for_event(self, table_name: str, event: TriggerEvent, timing: TriggerTiming) -> List[TriggerInfo]:
        """
        获取指定表、事件和时机的触发器
        
        Args:
            table_name: 表名
            event: 事件类型
            timing: 触发时机
            
        Returns:
            List[TriggerInfo]: 匹配的触发器列表
        """
        triggers = self.get_triggers_for_table(table_name)
        matching_triggers = []
        
        for trigger in triggers:
            if (trigger.timing == timing and 
                event in trigger.events):
                matching_triggers.append(trigger)
        
        return matching_triggers
    
    def get_all_triggers(self) -> List[TriggerInfo]:
        """
        获取所有触发器
        
        Returns:
            List[TriggerInfo]: 所有触发器列表
        """
        return list(self._triggers_by_name.values())
    
    def get_trigger_by_name(self, trigger_name: str) -> Optional[TriggerInfo]:
        """
        根据名称获取触发器
        
        Args:
            trigger_name: 触发器名称
            
        Returns:
            Optional[TriggerInfo]: 触发器信息，如果不存在返回None
        """
        return self._triggers_by_name.get(trigger_name)
    
    def execute_triggers(self, table_name: str, event: TriggerEvent, timing: TriggerTiming, 
                        old_data: Optional[Dict] = None, new_data: Optional[Dict] = None,
                        context: Optional[Dict] = None) -> bool:
        """
        执行触发器
        
        Args:
            table_name: 表名
            event: 事件类型
            timing: 触发时机
            old_data: 旧数据（UPDATE/DELETE时使用）
            new_data: 新数据（INSERT/UPDATE时使用）
            context: 执行上下文
            
        Returns:
            bool: 执行是否成功
        """
        try:
            triggers = self.get_triggers_for_event(table_name, event, timing)
            
            if not triggers:
                return True  # 没有触发器，直接返回成功
            
            logger.info(f"执行 {len(triggers)} 个触发器，表: {table_name}, 事件: {event.value}, 时机: {timing.value}")
            
            for trigger in triggers:
                # 记录执行历史
                execution_record = {
                    'trigger_name': trigger.name,
                    'table_name': table_name,
                    'event': event.value,
                    'timing': timing.value,
                    'old_data': old_data,
                    'new_data': new_data,
                    'timestamp': self._get_current_timestamp()
                }
                
                # 检查WHEN条件
                if trigger.when_condition and not self._condition_evaluator.evaluate_condition(
                    trigger.when_condition, old_data, new_data, context):
                    logger.debug(f"触发器 {trigger.name} WHEN条件不满足，跳过执行")
                    execution_record['skipped'] = True
                    self._execution_history.append(execution_record)
                    continue
                
                # 执行触发器主体
                success = self._execute_trigger_body(
                    trigger, old_data, new_data, context)
                
                execution_record['success'] = success
                self._execution_history.append(execution_record)
                
                if not success:
                    logger.error(f"触发器 {trigger.name} 执行失败")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"执行触发器时发生错误: {e}")
            return False
    
    def _execute_trigger_body(self, trigger: TriggerInfo, old_data: Optional[Dict], 
                            new_data: Optional[Dict], context: Optional[Dict]) -> bool:
        """
        执行触发器主体
        
        Args:
            trigger: 触发器信息
            old_data: 旧数据
            new_data: 新数据
            context: 执行上下文
            
        Returns:
            bool: 执行是否成功
        """
        try:
            # 这里需要与执行器集成，暂时返回True
            # 实际实现中，这里应该调用执行器来执行触发器主体中的SQL语句
            logger.debug(f"执行触发器主体: {trigger.name}")
            
            # TODO: 集成执行器来执行触发器主体
            # 1. 解析触发器主体中的SQL语句
            # 2. 替换OLD/NEW引用
            # 3. 调用执行器执行
            
            return True
            
        except Exception as e:
            logger.error(f"执行触发器主体失败: {e}")
            return False
    
    
    def _get_current_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def get_execution_history(self) -> List[Dict[str, Any]]:
        """
        获取触发器执行历史
        
        Returns:
            List[Dict[str, Any]]: 执行历史列表
        """
        return self._execution_history.copy()
    
    def clear_execution_history(self):
        """清空执行历史"""
        self._execution_history.clear()
    
    def get_trigger_statistics(self) -> Dict[str, Any]:
        """
        获取触发器统计信息
        
        Returns:
            Dict[str, Any]: 统计信息
        """
        total_triggers = len(self._triggers_by_name)
        triggers_by_timing = {}
        triggers_by_event = {}
        
        for trigger in self._triggers_by_name.values():
            # 统计按时机分组
            timing = trigger.timing.value
            triggers_by_timing[timing] = triggers_by_timing.get(timing, 0) + 1
            
            # 统计按事件分组
            for event in trigger.events:
                event_name = event.value
                triggers_by_event[event_name] = triggers_by_event.get(event_name, 0) + 1
        
        return {
            'total_triggers': total_triggers,
            'triggers_by_timing': triggers_by_timing,
            'triggers_by_event': triggers_by_event,
            'execution_count': len(self._execution_history)
        }
