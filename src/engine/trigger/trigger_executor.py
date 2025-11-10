# -*- coding: utf-8 -*-
"""
触发器执行器 - 负责执行触发器相关的操作
"""
from typing import Dict, Any
from src.engine.trigger.trigger_manager import TriggerManager, TriggerTiming, TriggerEvent, TriggerInfo
from src.engine.catalog_manager import CatalogManager
from src.engine.storage.storage_engine import StorageEngine
from src.sql_compiler.ast_nodes import CreateTriggerStatement, DropTriggerStatement, ShowTriggers
import logging

logger = logging.getLogger(__name__)

class TriggerExecutor:
    """触发器执行器"""
    
    def __init__(self, trigger_manager: TriggerManager, catalog_manager: CatalogManager, 
                 storage_engine: StorageEngine):
        self.trigger_manager = trigger_manager
        self.catalog_manager = catalog_manager
        self.storage_engine = storage_engine
    
    def execute_create_trigger(self, ast: CreateTriggerStatement) -> Dict[str, Any]:
        """
        执行CREATE TRIGGER语句
        
        Args:
            ast: CREATE TRIGGER AST节点
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        try:
            # 验证表是否存在
            table_name = ast.table_name.value if hasattr(ast.table_name, 'value') else str(ast.table_name)
            if not self.catalog_manager.table_exists(table_name):
                return {
                    "success": False,
                    "message": f"表 {table_name} 不存在"
                }
            
            # 转换AST为TriggerInfo
            trigger_info = self._convert_ast_to_trigger_info(ast)
            
            # 创建触发器
            success = self.trigger_manager.create_trigger(trigger_info)
            
            if success:
                return {
                    "success": True,
                    "message": f"触发器 {trigger_info.name} 创建成功"
                }
            else:
                return {
                    "success": False,
                    "message": f"触发器 {trigger_info.name} 创建失败"
                }
                
        except Exception as e:
            logger.error(f"执行CREATE TRIGGER失败: {e}")
            return {
                "success": False,
                "message": f"CREATE TRIGGER执行失败: {str(e)}"
            }
    
    def execute_drop_trigger(self, ast: DropTriggerStatement) -> Dict[str, Any]:
        """
        执行DROP TRIGGER语句
        
        Args:
            ast: DROP TRIGGER AST节点
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        try:
            trigger_name = ast.trigger_name.value if hasattr(ast.trigger_name, 'value') else str(ast.trigger_name)
            
            success = self.trigger_manager.drop_trigger(trigger_name)
            
            if success:
                return {
                    "success": True,
                    "message": f"触发器 {trigger_name} 删除成功"
                }
            else:
                return {
                    "success": False,
                    "message": f"触发器 {trigger_name} 不存在或删除失败"
                }
                
        except Exception as e:
            logger.error(f"执行DROP TRIGGER失败: {e}")
            return {
                "success": False,
                "message": f"DROP TRIGGER执行失败: {str(e)}"
            }
    
    def execute_show_triggers(self, ast: ShowTriggers) -> Dict[str, Any]:
        """
        执行SHOW TRIGGERS语句
        
        Args:
            ast: SHOW TRIGGERS AST节点
            
        Returns:
            Dict[str, Any]: 执行结果
        """
        try:
            triggers = self.trigger_manager.get_all_triggers()
            
            # 格式化触发器信息
            trigger_list = []
            for trigger in triggers:
                trigger_info = {
                    "name": trigger.name,
                    "table": trigger.table_name,
                    "timing": trigger.timing.value,
                    "events": [event.value for event in trigger.events],
                    "row_level": trigger.is_row_level,
                    "when_condition": str(trigger.when_condition) if trigger.when_condition else None,
                    "created_at": trigger.created_at
                }
                trigger_list.append(trigger_info)
            
            return {
                "success": True,
                "data": trigger_list,
                "message": f"找到 {len(trigger_list)} 个触发器"
            }
            
        except Exception as e:
            logger.error(f"执行SHOW TRIGGERS失败: {e}")
            return {
                "success": False,
                "message": f"SHOW TRIGGERS执行失败: {str(e)}"
            }
    
    def _convert_ast_to_trigger_info(self, ast: CreateTriggerStatement) -> TriggerInfo:
        """
        将CREATE TRIGGER AST节点转换为TriggerInfo
        
        Args:
            ast: CREATE TRIGGER AST节点
            
        Returns:
            TriggerInfo: 触发器信息
        """
        # 获取触发器名称
        trigger_name = ast.trigger_name.value if hasattr(ast.trigger_name, 'value') else str(ast.trigger_name)
        
        # 获取表名
        table_name = ast.table_name.value if hasattr(ast.table_name, 'value') else str(ast.table_name)
        
        # 转换触发时机
        timing_map = {
            'BEFORE': TriggerTiming.BEFORE,
            'AFTER': TriggerTiming.AFTER,
            'INSTEAD OF': TriggerTiming.INSTEAD_OF
        }
        timing = timing_map.get(ast.timing, TriggerTiming.AFTER)
        
        # 转换事件类型
        event_map = {
            'INSERT': TriggerEvent.INSERT,
            'UPDATE': TriggerEvent.UPDATE,
            'DELETE': TriggerEvent.DELETE
        }
        events = [event_map[event] for event in ast.events if event in event_map]
        
        # 创建TriggerInfo
        trigger_info = TriggerInfo(
            name=trigger_name,
            table_name=table_name,
            timing=timing,
            events=events,
            is_row_level=ast.is_row_level,
            when_condition=ast.when_condition,
            trigger_body=ast.trigger_body or []
        )
        
        return trigger_info
    
    def fire_triggers_for_insert(self, table_name: str, new_data: Dict[str, Any]) -> bool:
        """
        为INSERT操作触发触发器
        
        Args:
            table_name: 表名
            new_data: 新插入的数据
            
        Returns:
            bool: 触发器执行是否成功
        """
        return self.trigger_manager.execute_triggers(
            table_name=table_name,
            event=TriggerEvent.INSERT,
            timing=TriggerTiming.BEFORE,
            new_data=new_data
        ) and self.trigger_manager.execute_triggers(
            table_name=table_name,
            event=TriggerEvent.INSERT,
            timing=TriggerTiming.AFTER,
            new_data=new_data
        )
    
    def fire_triggers_for_update(self, table_name: str, old_data: Dict[str, Any], 
                                new_data: Dict[str, Any]) -> bool:
        """
        为UPDATE操作触发触发器
        
        Args:
            table_name: 表名
            old_data: 更新前的数据
            new_data: 更新后的数据
            
        Returns:
            bool: 触发器执行是否成功
        """
        return self.trigger_manager.execute_triggers(
            table_name=table_name,
            event=TriggerEvent.UPDATE,
            timing=TriggerTiming.BEFORE,
            old_data=old_data,
            new_data=new_data
        ) and self.trigger_manager.execute_triggers(
            table_name=table_name,
            event=TriggerEvent.UPDATE,
            timing=TriggerTiming.AFTER,
            old_data=old_data,
            new_data=new_data
        )
    
    def fire_triggers_for_delete(self, table_name: str, old_data: Dict[str, Any]) -> bool:
        """
        为DELETE操作触发触发器
        
        Args:
            table_name: 表名
            old_data: 删除前的数据
            
        Returns:
            bool: 触发器执行是否成功
        """
        return self.trigger_manager.execute_triggers(
            table_name=table_name,
            event=TriggerEvent.DELETE,
            timing=TriggerTiming.BEFORE,
            old_data=old_data
        ) and self.trigger_manager.execute_triggers(
            table_name=table_name,
            event=TriggerEvent.DELETE,
            timing=TriggerTiming.AFTER,
            old_data=old_data
        )
