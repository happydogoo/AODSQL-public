# -*- coding: utf-8 -*-
"""
视图管理器 - 处理视图的创建、删除、修改和查询重写
"""
from typing import Optional, List
from src.engine.catalog_manager import CatalogManager, ViewInfo
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.sql_compiler.lexicalAnalysis import tokenize
from src.sql_compiler.new_syntax_analyzer import NewSyntaxAnalyzer
import re


class ViewManager:
    """视图管理器"""
    
    def __init__(self, catalog_manager: CatalogManager):
        self.catalog_manager = catalog_manager
        self.syntax_analyzer = NewSyntaxAnalyzer()
    
    def create_view(self, view_name: str, definition: str, creator: str = 'system', 
                   schema_name: str = 'public', is_updatable: bool = False) -> bool:
        """
        创建视图
        
        Args:
            view_name: 视图名称
            definition: 视图的SELECT语句定义
            creator: 创建者
            schema_name: 模式名称
            is_updatable: 是否可更新
            
        Returns:
            bool: 创建是否成功
        """
        try:
            # 1. 验证视图定义语法
            if not self._validate_view_definition(definition):
                raise Exception("视图定义语法错误")
            
            # 2. 检查视图名称是否已存在
            if self.catalog_manager.view_exists(view_name):
                raise Exception(f"视图 '{view_name}' 已存在")
            
            # 3. 检查权限（简化版：检查引用的表是否存在）
            if not self._check_view_permissions(definition):
                raise Exception("权限检查失败：引用的表不存在或无权限访问")
            
            # 4. 创建视图
            self.catalog_manager.create_view(
                view_name,
                definition,
                schema_name,
                creator,
                is_updatable
            )
            
            print(f"✅ 视图 '{view_name}' 创建成功")
            return True
            
        except Exception as e:
            print(f"❌ 创建视图失败: {str(e)}")
            return False
    
    def drop_view(self, view_name: str) -> bool:
        """
        删除视图
        
        Args:
            view_name: 视图名称
            
        Returns:
            bool: 删除是否成功
        """
        try:
            # 检查视图是否存在
            if not self.catalog_manager.view_exists(view_name):
                raise Exception(f"视图 '{view_name}' 不存在")
            
            # 检查依赖关系（简化版：暂时不检查）
            # TODO: 实现依赖关系检查
            
            # 删除视图
            self.catalog_manager.delete_view(view_name)
            
            print(f"✅ 视图 '{view_name}' 删除成功")
            return True
            
        except Exception as e:
            print(f"❌ 删除视图失败: {str(e)}")
            return False
    
    def alter_view(self, view_name: str, definition: str, is_updatable: bool = None) -> bool:
        """
        修改视图
        
        Args:
            view_name: 视图名称
            definition: 新的SELECT语句定义
            is_updatable: 是否可更新（可选）
            
        Returns:
            bool: 修改是否成功
        """
        try:
            # 1. 验证视图定义语法
            if not self._validate_view_definition(definition):
                raise Exception("视图定义语法错误")
            
            # 2. 检查视图是否存在
            if not self.catalog_manager.view_exists(view_name):
                raise Exception(f"视图 '{view_name}' 不存在")
            
            # 3. 检查权限
            if not self._check_view_permissions(definition):
                raise Exception("权限检查失败：引用的表不存在或无权限访问")
            
            # 4. 更新视图
            self.catalog_manager.update_view(view_name, definition, is_updatable)
            
            print(f"✅ 视图 '{view_name}' 修改成功")
            return True
            
        except Exception as e:
            print(f"❌ 修改视图失败: {str(e)}")
            return False
    
    def get_view_definition(self, view_name: str) -> Optional[str]:
        """
        获取视图定义
        
        Args:
            view_name: 视图名称
            
        Returns:
            str: 视图定义，如果不存在返回None
        """
        try:
            view_info = self.catalog_manager.get_view(view_name)
            return view_info.definition
        except:
            return None
    
    def is_view_updatable(self, view_name: str) -> bool:
        """
        检查视图是否可更新
        
        Args:
            view_name: 视图名称
            
        Returns:
            bool: 是否可更新
        """
        return self.catalog_manager.is_view_updatable(view_name)
    
    def list_views(self) -> List[str]:
        """
        列出所有视图
        
        Returns:
            List[str]: 视图名称列表
        """
        return self.catalog_manager.list_views()
    
    def _validate_view_definition(self, definition: str) -> bool:
        """
        验证视图定义语法
        
        Args:
            definition: SELECT语句定义
            
        Returns:
            bool: 语法是否正确
        """
        try:
            # 使用词法分析器检查语法
            tokens = tokenize(definition)
            if not tokens:
                return False
            
            # 检查是否以SELECT开头
            if tokens[0][1].upper() != 'SELECT':
                return False
            
            # 使用语法分析器验证
            ast = self.syntax_analyzer.build_ast_from_tokens(tokens)
            return ast is not None
            
        except Exception as e:
            print(f"语法验证失败: {str(e)}")
            return False
    
    def _check_view_permissions(self, definition: str) -> bool:
        """
        检查视图权限（简化版：只检查引用的表是否存在）
        
        Args:
            definition: SELECT语句定义
            
        Returns:
            bool: 权限检查是否通过
        """
        try:
            # 提取FROM子句中的表名
            table_names = self._extract_table_names(definition)
            
            # 检查每个表是否存在
            for table_name in table_names:
                if not self.catalog_manager.table_exists(table_name):
                    print(f"警告: 表 '{table_name}' 不存在")
                    return False
            
            return True
            
        except Exception as e:
            print(f"权限检查失败: {str(e)}")
            return False
    
    def _extract_table_names(self, definition: str) -> List[str]:
        """
        从SELECT语句中提取表名
        
        Args:
            definition: SELECT语句
            
        Returns:
            List[str]: 表名列表
        """
        table_names = []
        
        try:
            # 使用正则表达式提取FROM子句中的表名
            # 简化版：只处理基本的FROM table_name格式
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
    
    def get_view_info(self, view_name: str) -> Optional[ViewInfo]:
        """
        获取视图详细信息
        
        Args:
            view_name: 视图名称
            
        Returns:
            ViewInfo: 视图信息，如果不存在返回None
        """
        try:
            return self.catalog_manager.get_view(view_name)
        except:
            return None
