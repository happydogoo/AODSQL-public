# -*- coding: utf-8 -*-
"""
可更新视图管理器 - 处理可更新视图的INSERT、UPDATE、DELETE操作
"""
from typing import Dict, List, Optional, Tuple, Any
from src.engine.catalog_manager import CatalogManager
from src.engine.view.view_manager import ViewManager
from src.engine.view.query_rewriter import QueryRewriter
import re


class UpdatableViewManager:
    """可更新视图管理器"""
    
    def __init__(self, catalog_manager: CatalogManager):
        self.catalog_manager = catalog_manager
        self.view_manager = ViewManager(catalog_manager)
        self.query_rewriter = QueryRewriter(catalog_manager)
    
    def is_view_updatable(self, view_name: str) -> bool:
        """
        检查视图是否可更新
        
        Args:
            view_name: 视图名
            
        Returns:
            bool: 是否可更新
        """
        try:
            if not self.catalog_manager.view_exists(view_name):
                return False
            
            view_info = self.catalog_manager.get_view(view_name)
            return view_info.is_updatable
            
        except Exception as e:
            print(f"❌ 检查视图可更新性失败: {str(e)}")
            return False
    
    def validate_view_updatability(self, view_name: str) -> Tuple[bool, str]:
        """
        验证视图是否可更新
        
        Args:
            view_name: 视图名
            
        Returns:
            Tuple[bool, str]: (是否可更新, 原因)
        """
        try:
            if not self.catalog_manager.view_exists(view_name):
                return False, "视图不存在"
            
            view_info = self.catalog_manager.get_view(view_name)
            definition = view_info.definition
            
            # 检查视图定义是否满足可更新条件
            return self._check_updatability_conditions(definition)
            
        except Exception as e:
            return False, f"验证失败: {str(e)}"
    
    def _check_updatability_conditions(self, definition: str) -> Tuple[bool, str]:
        """
        检查视图定义是否满足可更新条件
        
        Args:
            definition: 视图定义
            
        Returns:
            Tuple[bool, str]: (是否可更新, 原因)
        """
        try:
            # 1. 检查是否只涉及一个表
            table_names = self._extract_table_names_from_definition(definition)
            if len(table_names) > 1:
                return False, "视图涉及多个表，不可更新"
            
            # 2. 检查是否包含JOIN
            if re.search(r'\bJOIN\b', definition, re.IGNORECASE):
                return False, "视图包含JOIN，不可更新"
            
            # 3. 检查是否包含聚合函数
            aggregate_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT']
            for func in aggregate_functions:
                if re.search(rf'\b{func}\s*\(', definition, re.IGNORECASE):
                    return False, f"视图包含聚合函数 {func}，不可更新"
            
            # 4. 检查是否包含GROUP BY
            if re.search(r'\bGROUP\s+BY\b', definition, re.IGNORECASE):
                return False, "视图包含GROUP BY，不可更新"
            
            # 5. 检查是否包含HAVING
            if re.search(r'\bHAVING\b', definition, re.IGNORECASE):
                return False, "视图包含HAVING，不可更新"
            
            # 6. 检查是否包含DISTINCT
            if re.search(r'\bDISTINCT\b', definition, re.IGNORECASE):
                return False, "视图包含DISTINCT，不可更新"
            
            # 7. 检查是否包含子查询
            if re.search(r'\(.*SELECT.*\)', definition, re.IGNORECASE):
                return False, "视图包含子查询，不可更新"
            
            return True, "视图满足可更新条件"
            
        except Exception as e:
            return False, f"检查条件失败: {str(e)}"
    
    def _extract_table_names_from_definition(self, definition: str) -> List[str]:
        """从视图定义中提取表名"""
        table_names = []
        
        try:
            # 使用正则表达式提取FROM子句中的表名
            from_pattern = r'FROM\s+(\w+)'
            matches = re.findall(from_pattern, definition.upper())
            table_names.extend(matches)
            
        except Exception as e:
            print(f"提取表名失败: {str(e)}")
        
        return table_names
    
    def set_view_updatable(self, view_name: str, is_updatable: bool) -> bool:
        """
        设置视图是否可更新
        
        Args:
            view_name: 视图名
            is_updatable: 是否可更新
            
        Returns:
            bool: 是否成功
        """
        try:
            if not self.catalog_manager.view_exists(view_name):
                print(f"❌ 视图 '{view_name}' 不存在")
                return False
            
            if is_updatable:
                # 验证视图是否满足可更新条件
                valid, reason = self.validate_view_updatability(view_name)
                if not valid:
                    print(f"❌ 视图 '{view_name}' 不可更新: {reason}")
                    return False
            
            # 更新视图的可更新状态
            view_info = self.catalog_manager.get_view(view_name)
            self.catalog_manager.update_view(view_name, view_info.definition, is_updatable)
            
            print(f"✅ 视图 '{view_name}' 已设置为{'可更新' if is_updatable else '不可更新'}")
            return True
            
        except Exception as e:
            print(f"❌ 设置视图可更新性失败: {str(e)}")
            return False
    
    def rewrite_view_insert(self, view_name: str, insert_sql: str) -> Optional[str]:
        """
        重写视图INSERT操作为底层表操作
        
        Args:
            view_name: 视图名
            insert_sql: 原始INSERT语句
            
        Returns:
            str: 重写后的INSERT语句
        """
        try:
            if not self.is_view_updatable(view_name):
                print(f"❌ 视图 '{view_name}' 不可更新")
                return None
            
            # 获取视图定义
            view_info = self.catalog_manager.get_view(view_name)
            definition = view_info.definition
            
            # 提取底层表名
            table_names = self._extract_table_names_from_definition(definition)
            if len(table_names) != 1:
                print(f"❌ 视图 '{view_name}' 涉及多个表，无法重写INSERT")
                return None
            
            base_table = table_names[0]
            
            # 重写INSERT语句
            rewritten_sql = re.sub(
                rf'\bINTO\s+{re.escape(view_name)}\b',
                f'INTO {base_table}',
                insert_sql,
                flags=re.IGNORECASE
            )
            
            print(f"🔄 视图INSERT重写: {insert_sql} -> {rewritten_sql}")
            return rewritten_sql
            
        except Exception as e:
            print(f"❌ 重写视图INSERT失败: {str(e)}")
            return None
    
    def rewrite_view_update(self, view_name: str, update_sql: str) -> Optional[str]:
        """
        重写视图UPDATE操作为底层表操作
        
        Args:
            view_name: 视图名
            update_sql: 原始UPDATE语句
            
        Returns:
            str: 重写后的UPDATE语句
        """
        try:
            if not self.is_view_updatable(view_name):
                print(f"❌ 视图 '{view_name}' 不可更新")
                return None
            
            # 获取视图定义
            view_info = self.catalog_manager.get_view(view_name)
            definition = view_info.definition
            
            # 提取底层表名
            table_names = self._extract_table_names_from_definition(definition)
            if len(table_names) != 1:
                print(f"❌ 视图 '{view_name}' 涉及多个表，无法重写UPDATE")
                return None
            
            base_table = table_names[0]
            
            # 重写UPDATE语句
            rewritten_sql = re.sub(
                rf'\bUPDATE\s+{re.escape(view_name)}\b',
                f'UPDATE {base_table}',
                update_sql,
                flags=re.IGNORECASE
            )
            
            print(f"🔄 视图UPDATE重写: {update_sql} -> {rewritten_sql}")
            return rewritten_sql
            
        except Exception as e:
            print(f"❌ 重写视图UPDATE失败: {str(e)}")
            return None
    
    def rewrite_view_delete(self, view_name: str, delete_sql: str) -> Optional[str]:
        """
        重写视图DELETE操作为底层表操作
        
        Args:
            view_name: 视图名
            delete_sql: 原始DELETE语句
            
        Returns:
            str: 重写后的DELETE语句
        """
        try:
            if not self.is_view_updatable(view_name):
                print(f"❌ 视图 '{view_name}' 不可更新")
                return None
            
            # 获取视图定义
            view_info = self.catalog_manager.get_view(view_name)
            definition = view_info.definition
            
            # 提取底层表名
            table_names = self._extract_table_names_from_definition(definition)
            if len(table_names) != 1:
                print(f"❌ 视图 '{view_name}' 涉及多个表，无法重写DELETE")
                return None
            
            base_table = table_names[0]
            
            # 重写DELETE语句
            rewritten_sql = re.sub(
                rf'\bFROM\s+{re.escape(view_name)}\b',
                f'FROM {base_table}',
                delete_sql,
                flags=re.IGNORECASE
            )
            
            print(f"🔄 视图DELETE重写: {delete_sql} -> {rewritten_sql}")
            return rewritten_sql
            
        except Exception as e:
            print(f"❌ 重写视图DELETE失败: {str(e)}")
            return None
    
    def get_updatable_views(self) -> List[str]:
        """
        获取所有可更新的视图
        
        Returns:
            List[str]: 可更新视图列表
        """
        try:
            updatable_views = []
            for view_name in self.catalog_manager.list_views():
                if self.is_view_updatable(view_name):
                    updatable_views.append(view_name)
            return updatable_views
        except Exception as e:
            print(f"❌ 获取可更新视图失败: {str(e)}")
            return []
    
    def analyze_view_dependencies(self, view_name: str) -> Dict[str, Any]:
        """
        分析视图依赖关系
        
        Args:
            view_name: 视图名
            
        Returns:
            Dict[str, Any]: 依赖关系信息
        """
        try:
            if not self.catalog_manager.view_exists(view_name):
                return {"error": "视图不存在"}
            
            view_info = self.catalog_manager.get_view(view_name)
            definition = view_info.definition
            
            # 分析依赖
            dependencies = {
                "view_name": view_name,
                "base_tables": self._extract_table_names_from_definition(definition),
                "is_updatable": self.is_view_updatable(view_name),
                "updatability_reason": "",
                "complexity_score": self._calculate_complexity_score(definition)
            }
            
            # 验证可更新性
            if dependencies["is_updatable"]:
                valid, reason = self.validate_view_updatability(view_name)
                dependencies["updatability_reason"] = reason
                dependencies["is_updatable"] = valid
            
            return dependencies
            
        except Exception as e:
            return {"error": str(e)}
    
    def _calculate_complexity_score(self, definition: str) -> int:
        """
        计算视图定义的复杂度分数
        
        Args:
            definition: 视图定义
            
        Returns:
            int: 复杂度分数
        """
        score = 0
        
        # 基础分数
        score += 1
        
        # JOIN增加复杂度
        if re.search(r'\bJOIN\b', definition, re.IGNORECASE):
            score += 2
        
        # 聚合函数增加复杂度
        aggregate_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT']
        for func in aggregate_functions:
            if re.search(rf'\b{func}\s*\(', definition, re.IGNORECASE):
                score += 1
        
        # 子查询增加复杂度
        if re.search(r'\(.*SELECT.*\)', definition, re.IGNORECASE):
            score += 2
        
        # 复杂条件增加复杂度
        if re.search(r'\b(OR|AND)\b', definition, re.IGNORECASE):
            score += 1
        
        return score

