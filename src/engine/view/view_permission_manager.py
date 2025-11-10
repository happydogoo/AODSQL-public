# -*- coding: utf-8 -*-
"""
视图权限管理器 - 处理视图的权限和安全性控制
"""
from typing import Dict, List, Set, Optional, Tuple, Any
from src.engine.catalog_manager import CatalogManager, ViewInfo
from .view_manager import ViewManager
import re


class ViewPermissionManager:
    """视图权限管理器"""
    
    def __init__(self, catalog_manager: CatalogManager):
        self.catalog_manager = catalog_manager
        self.view_manager = ViewManager(catalog_manager)
        
        # 权限映射：用户 -> 权限集合
        self.user_permissions: Dict[str, Set[str]] = {}
        
        # 视图权限映射：视图名 -> 用户权限
        self.view_permissions: Dict[str, Dict[str, Set[str]]] = {}
        
        # 表权限映射：表名 -> 用户权限
        self.table_permissions: Dict[str, Dict[str, Set[str]]] = {}
    
    def grant_view_permission(self, user: str, view_name: str, permission: str) -> bool:
        """
        授予用户视图权限
        
        Args:
            user: 用户名
            view_name: 视图名
            permission: 权限类型 (SELECT, INSERT, UPDATE, DELETE)
            
        Returns:
            bool: 是否成功
        """
        try:
            # 检查视图是否存在
            if not self.catalog_manager.view_exists(view_name):
                print(f"❌ 视图 '{view_name}' 不存在")
                return False
            
            # 检查用户是否有底层表的权限
            if not self._check_underlying_table_permissions(user, view_name, permission):
                print(f"❌ 用户 '{user}' 没有视图 '{view_name}' 底层表的权限")
                return False
            
            # 授予权限
            if view_name not in self.view_permissions:
                self.view_permissions[view_name] = {}
            if user not in self.view_permissions[view_name]:
                self.view_permissions[view_name][user] = set()
            
            self.view_permissions[view_name][user].add(permission)
            
            # 更新用户权限
            if user not in self.user_permissions:
                self.user_permissions[user] = set()
            self.user_permissions[user].add(f"{view_name}:{permission}")
            
            print(f"✅ 已授予用户 '{user}' 对视图 '{view_name}' 的 '{permission}' 权限")
            return True
            
        except Exception as e:
            print(f"❌ 授予权限失败: {str(e)}")
            return False
    
    def revoke_view_permission(self, user: str, view_name: str, permission: str) -> bool:
        """
        撤销用户视图权限
        
        Args:
            user: 用户名
            view_name: 视图名
            permission: 权限类型
            
        Returns:
            bool: 是否成功
        """
        try:
            if (view_name in self.view_permissions and 
                user in self.view_permissions[view_name] and 
                permission in self.view_permissions[view_name][user]):
                
                self.view_permissions[view_name][user].remove(permission)
                
                # 更新用户权限
                if user in self.user_permissions:
                    self.user_permissions[user].discard(f"{view_name}:{permission}")
                
                print(f"✅ 已撤销用户 '{user}' 对视图 '{view_name}' 的 '{permission}' 权限")
                return True
            else:
                print(f"❌ 用户 '{user}' 没有视图 '{view_name}' 的 '{permission}' 权限")
                return False
                
        except Exception as e:
            print(f"❌ 撤销权限失败: {str(e)}")
            return False
    
    def check_view_permission(self, user: str, view_name: str, permission: str) -> bool:
        """
        检查用户是否有视图权限
        
        Args:
            user: 用户名
            view_name: 视图名
            permission: 权限类型
            
        Returns:
            bool: 是否有权限
        """
        try:
            # 检查视图是否存在
            if not self.catalog_manager.view_exists(view_name):
                return False
            
            # 检查直接权限
            if (view_name in self.view_permissions and 
                user in self.view_permissions[view_name] and 
                permission in self.view_permissions[view_name][user]):
                return True
            
            # 检查底层表权限
            return self._check_underlying_table_permissions(user, view_name, permission)
            
        except Exception as e:
            print(f"❌ 权限检查失败: {str(e)}")
            return False
    
    def _check_underlying_table_permissions(self, user: str, view_name: str, permission: str) -> bool:
        """
        检查用户是否有视图底层表的权限
        
        Args:
            user: 用户名
            view_name: 视图名
            permission: 权限类型
            
        Returns:
            bool: 是否有权限
        """
        try:
            # 获取视图定义
            view_definition = self.view_manager.get_view_definition(view_name)
            if not view_definition:
                return False
            
            # 提取底层表名
            table_names = self._extract_table_names_from_definition(view_definition)
            
            # 检查每个表的权限
            for table_name in table_names:
                if not self._check_table_permission(user, table_name, permission):
                    return False
            
            return True
            
        except Exception as e:
            print(f"❌ 底层表权限检查失败: {str(e)}")
            return False
    
    def _check_table_permission(self, user: str, table_name: str, permission: str) -> bool:
        """
        检查用户是否有表权限
        
        Args:
            user: 用户名
            table_name: 表名
            permission: 权限类型
            
        Returns:
            bool: 是否有权限
        """
        # 简化实现：假设所有用户都有所有表的权限
        # 在实际实现中，应该检查表权限映射
        return True
    
    def _extract_table_names_from_definition(self, definition: str) -> List[str]:
        """从视图定义中提取表名"""
        table_names = []
        
        try:
            # 使用正则表达式提取FROM子句中的表名
            from_pattern = r'FROM\s+(\w+)'
            matches = re.findall(from_pattern, definition.upper())
            table_names.extend(matches)
            
            # 处理JOIN子句中的表名
            join_pattern = r'JOIN\s+(\w+)'
            join_matches = re.findall(join_pattern, definition.upper())
            table_names.extend(join_matches)
            
        except Exception as e:
            print(f"提取表名失败: {str(e)}")
        
        return table_names
    
    def get_user_permissions(self, user: str) -> Dict[str, Set[str]]:
        """
        获取用户的所有权限
        
        Args:
            user: 用户名
            
        Returns:
            Dict[str, Set[str]]: 权限映射
        """
        return self.view_permissions.get(user, {})
    
    def get_view_permissions(self, view_name: str) -> Dict[str, Set[str]]:
        """
        获取视图的所有用户权限
        
        Args:
            view_name: 视图名
            
        Returns:
            Dict[str, Set[str]]: 用户权限映射
        """
        return self.view_permissions.get(view_name, {})
    
    def list_view_permissions(self) -> Dict[str, Dict[str, Set[str]]]:
        """
        列出所有视图权限
        
        Returns:
            Dict[str, Dict[str, Set[str]]]: 所有视图权限
        """
        return self.view_permissions.copy()
    
    def validate_view_creation_permissions(self, user: str, view_definition: str) -> Tuple[bool, str]:
        """
        验证用户是否有创建视图的权限
        
        Args:
            user: 用户名
            view_definition: 视图定义
            
        Returns:
            Tuple[bool, str]: (是否有权限, 错误信息)
        """
        try:
            # 提取视图定义中引用的表
            table_names = self._extract_table_names_from_definition(view_definition)
            
            # 检查每个表的SELECT权限
            for table_name in table_names:
                if not self._check_table_permission(user, table_name, 'SELECT'):
                    return False, f"用户 '{user}' 没有表 '{table_name}' 的SELECT权限"
            
            return True, "权限验证通过"
            
        except Exception as e:
            return False, f"权限验证失败: {str(e)}"
    
    def check_view_security(self, view_name: str) -> Dict[str, Any]:
        """
        检查视图安全性
        
        Args:
            view_name: 视图名
            
        Returns:
            Dict[str, Any]: 安全性信息
        """
        try:
            if not self.catalog_manager.view_exists(view_name):
                return {"secure": False, "error": "视图不存在"}
            
            view_info = self.catalog_manager.get_view(view_name)
            
            # 检查视图定义是否包含敏感信息
            security_issues = []
            
            # 检查是否包含密码字段
            if re.search(r'password|pwd|pass', view_info.definition, re.IGNORECASE):
                security_issues.append("视图定义包含密码字段")
            
            # 检查是否包含敏感表
            sensitive_tables = ['user', 'admin', 'system', 'config']
            table_names = self._extract_table_names_from_definition(view_info.definition)
            for table_name in table_names:
                if any(sensitive in table_name.lower() for sensitive in sensitive_tables):
                    security_issues.append(f"视图引用了敏感表: {table_name}")
            
            return {
                "secure": len(security_issues) == 0,
                "issues": security_issues,
                "view_name": view_name,
                "creator": view_info.creator,
                "is_updatable": view_info.is_updatable
            }
            
        except Exception as e:
            return {"secure": False, "error": str(e)}
    
    def audit_view_access(self, user: str, view_name: str, operation: str) -> bool:
        """
        审计视图访问
        
        Args:
            user: 用户名
            view_name: 视图名
            operation: 操作类型
            
        Returns:
            bool: 是否允许访问
        """
        try:
            # 检查权限
            if not self.check_view_permission(user, view_name, operation):
                print(f"❌ 审计失败: 用户 '{user}' 没有视图 '{view_name}' 的 '{operation}' 权限")
                return False
            
            # 记录访问日志（简化实现）
            print(f"📝 审计日志: 用户 '{user}' 对视图 '{view_name}' 执行 '{operation}' 操作")
            
            return True
            
        except Exception as e:
            print(f"❌ 审计失败: {str(e)}")
            return False

