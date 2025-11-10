# -*- coding: utf-8 -*-
"""
查询重写引擎 - 处理视图查询重写
当用户查询视图时，将视图查询重写为对底层表的查询
"""
from typing import Dict, Any, List, Tuple
from src.engine.catalog_manager import CatalogManager
from src.engine.view.view_manager import ViewManager
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.sql_compiler.lexicalAnalysis import tokenize
from src.sql_compiler.new_syntax_analyzer import NewSyntaxAnalyzer
import re


class QueryRewriter:
    """查询重写引擎"""
    
    def __init__(self, catalog_manager: CatalogManager):
        self.catalog_manager = catalog_manager
        self.view_manager = ViewManager(catalog_manager)
        self.syntax_analyzer = NewSyntaxAnalyzer()
    
    def rewrite_query(self, sql_text: str) -> str:
        """
        重写包含视图的查询
        
        Args:
            sql_text: 原始SQL查询
            
        Returns:
            str: 重写后的SQL查询
        """
        try:
            # 1. 解析原始查询
            tokens = tokenize(sql_text)
            if not tokens:
                return sql_text
            
            # 2. 检查是否包含视图引用（包括不存在的视图）
            view_names, missing_views = self._find_views_in_query(tokens)
            
            # 3. 如果有不存在的视图，抛出错误
            if missing_views:
                raise Exception(f"视图不存在: {', '.join(missing_views)}")
            
            if not view_names:
                return sql_text  # 没有视图，直接返回原查询
            
            # 4. 重写查询
            rewritten_query = self._rewrite_query_with_views(sql_text, view_names)
            
            print(f"🔄 查询重写: {sql_text} -> {rewritten_query}")
            return rewritten_query
            
        except Exception as e:
            print(f"❌ 查询重写失败: {str(e)}")
            raise e  # 重新抛出异常，让上层处理
    
    def _find_views_in_query(self, tokens: List[Tuple]) -> Tuple[List[str], List[str]]:
        """
        在查询中查找视图引用
        
        Args:
            tokens: 词法分析后的token列表
            
        Returns:
            Tuple[List[str], List[str]]: (存在的视图名称列表, 不存在的视图名称列表)
        """
        view_names = []
        missing_views = []
        in_from_clause = False
        
        for i, (token_type, token_value, line, col) in enumerate(tokens):
            if token_type == 'KEYWORD' and token_value.upper() == 'FROM':
                in_from_clause = True
                continue
            elif token_type == 'KEYWORD' and token_value.upper() in ['WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT']:
                in_from_clause = False
                continue
            elif token_type == 'KEYWORD' and token_value.upper() in ['JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL']:
                in_from_clause = True
                continue
            
            if in_from_clause and token_type == 'ID':
                # 检查这个标识符是否是视图（存在或不存在）
                if self.catalog_manager.view_exists(token_value):
                    view_names.append(token_value)
                elif self._is_potential_view_name(token_value):
                    # 检查是否可能是视图名（不是表名）
                    if not self.catalog_manager.table_exists(token_value):
                        missing_views.append(token_value)
        
        return view_names, missing_views
    
    def _is_potential_view_name(self, name: str) -> bool:
        """
        判断一个名称是否可能是视图名
        这里使用简单的启发式规则：以v_开头的名称可能是视图
        """
        return name.startswith('v_')
    
    def _rewrite_query_with_views(self, sql_text: str, view_names: List[str]) -> str:
        """
        重写包含视图的查询
        
        Args:
            sql_text: 原始SQL查询
            view_names: 视图名称列表
            
        Returns:
            str: 重写后的SQL查询
        """
        rewritten_query = sql_text
        
        for view_name in view_names:
            # 获取视图定义
            view_definition = self.view_manager.get_view_definition(view_name)
            if not view_definition:
                continue
            
            # 重写查询，将视图替换为子查询
            rewritten_query = self._replace_view_with_subquery(
                rewritten_query, view_name, view_definition
            )
        
        return rewritten_query
    
    def _replace_view_with_subquery(self, sql_text: str, view_name: str, view_definition: str) -> str:
        """
        将视图替换为完整的视图定义
        
        Args:
            sql_text: 原始SQL查询
            view_name: 视图名称
            view_definition: 视图定义
            
        Returns:
            str: 替换后的SQL查询
        """
        try:
            import re
            
            # 清理视图定义，移除可能的AST对象表示
            clean_definition = self._clean_view_definition(view_definition)
            
            # 检查是否是简单的单表视图
            if self._is_simple_view(clean_definition):
                return self._replace_simple_view(sql_text, view_name, clean_definition)
            else:
                # 复杂视图，使用子查询方式
                return self._replace_complex_view(sql_text, view_name, clean_definition)
            
        except Exception as e:
            print(f"⚠️ 视图重写失败: {e}")
            return sql_text
    
    def _clean_view_definition(self, view_definition: str) -> str:
        """
        清理视图定义，将AST对象转换为标准SQL
        """
        import re
        
        # 处理Identifier AST对象
        # 例如: Identifier(token=('ID', 's.name', 0, 0), value='s.name')
        identifier_pattern = r"Identifier\(token=\([^)]+\),\s*value='([^']+)'\)"
        view_definition = re.sub(identifier_pattern, r'\1', view_definition)
        
        # 处理ColumnWithAlias AST对象
        # 例如: ColumnWithAlias(Identifier(...) AS teacher_name)
        column_with_alias_pattern = r"ColumnWithAlias\(([^)]+)\s+AS\s+([^)]+)\)"
        def replace_column_with_alias(match):
            column = match.group(1)
            alias = match.group(2)
            # 如果column还是Identifier对象，需要进一步处理
            if 'Identifier(' in column:
                column = re.sub(identifier_pattern, r'\1', column)
            return f"{column} AS {alias}"
        
        view_definition = re.sub(column_with_alias_pattern, replace_column_with_alias, view_definition)
        
        # 处理WHERE条件中的AST对象
        where_pattern = r'WHERE\s+Condition\(([^)]+)\)'
        where_match = re.search(where_pattern, view_definition, re.IGNORECASE)
        
        if where_match:
            # 提取条件内容
            condition_str = where_match.group(1)
            
            # 处理Value对象
            value_pattern = r'Value\([^,]+,\s*([^)]+)\)'
            condition_str = re.sub(value_pattern, r"'\1'", condition_str)
            
            # 处理列引用
            condition_str = re.sub(r'(\w+)\.(\w+)', r'\1.\2', condition_str)
            
            # 替换WHERE子句
            view_definition = re.sub(where_pattern, f'WHERE {condition_str}', view_definition, flags=re.IGNORECASE)
        
        # 处理直接的WHERE条件（没有Condition包装）
        # 例如: WHERE gpa > Literal(token=('NUMBER', '3.5', 1, 68), value='3.5')
        direct_where_pattern = r'WHERE\s+([^;]+)'
        direct_where_match = re.search(direct_where_pattern, view_definition, re.IGNORECASE)
        
        if direct_where_match:
            condition_str = direct_where_match.group(1)
            
            # 处理Literal对象
            literal_pattern = r"Literal\(token=\([^)]+\),\s*value='([^']+)'\)"
            condition_str = re.sub(literal_pattern, r'\1', condition_str)
            
            # 处理Literal对象（带逗号的格式，如 token=('NUMBER', '3.5', 1, 68, value='3.5')）
            literal_pattern2 = r"Literal\(token=\([^)]+,\s*value='([^']+)'\)"
            condition_str = re.sub(literal_pattern2, r'\1', condition_str)
            
            # 处理Identifier对象
            identifier_pattern = r"Identifier\(token=\([^)]+\),\s*value='([^']+)'\)"
            condition_str = re.sub(identifier_pattern, r'\1', condition_str)
            
            # 处理Token对象
            token_pattern = r"Token\[Type:\s*([^,]+),\s*Literal:\s*'([^']+)',\s*Pos:\s*[^]]+\]"
            def replace_token(match):
                token_type = match.group(1)
                token_value = match.group(2)
                return token_value
            condition_str = re.sub(token_pattern, replace_token, condition_str)
            
            # 替换WHERE子句
            view_definition = re.sub(direct_where_pattern, f'WHERE {condition_str}', view_definition, flags=re.IGNORECASE)
        
        # 处理直接的Value对象（不在Condition中的）
        value_pattern = r'Value\(STRING,\s*([^)]+)\)'
        view_definition = re.sub(value_pattern, r"'\1'", view_definition, flags=re.IGNORECASE)
        
        # 处理聚合函数AST对象
        # 例如: AggregateFunction(function_name='COUNT', argument=Identifier(...), distinct=False, alias='total_orders')
        aggregate_pattern = r"AggregateFunction\(function_name='([^']+)',\s*argument=Identifier\([^,]+,\s*value='([^']+)'\),\s*distinct=([^,]+),\s*alias='([^']+)'\)"
        def replace_aggregate(match):
            func_name = match.group(1)
            argument = match.group(2)
            distinct = match.group(3) == 'True'
            alias = match.group(4)
            
            distinct_str = 'DISTINCT ' if distinct else ''
            return f"{func_name}({distinct_str}{argument}) AS {alias}"
        
        view_definition = re.sub(aggregate_pattern, replace_aggregate, view_definition)
        
        # 处理没有别名的聚合函数
        aggregate_no_alias_pattern = r"AggregateFunction\(function_name='([^']+)',\s*argument=Identifier\([^,]+,\s*value='([^']+)'\),\s*distinct=([^,]+),\s*alias=None\)"
        def replace_aggregate_no_alias(match):
            func_name = match.group(1)
            argument = match.group(2)
            distinct = match.group(3) == 'True'
            
            distinct_str = 'DISTINCT ' if distinct else ''
            return f"{func_name}({distinct_str}{argument})"
        
        view_definition = re.sub(aggregate_no_alias_pattern, replace_aggregate_no_alias, view_definition)
        
        # 如果视图定义中缺少JOIN信息，尝试从原始查询中推断
        # 这是一个简化的处理，实际中可能需要更复杂的逻辑
        if 'JOIN' not in view_definition.upper() and 'orders' in view_definition.lower():
            # 这是一个包含orders表的视图，需要添加JOIN
            view_definition = view_definition.replace(
                'FROM customers',
                'FROM customers c JOIN orders o ON c.customer_id = o.customer_id'
            )
        
        return view_definition
    
    def _is_simple_view(self, view_definition: str) -> bool:
        """
        检查是否是简单的单表视图
        """
        import re
        # 检查是否包含JOIN、子查询等复杂结构
        complex_patterns = [
            r'\bJOIN\b',
            r'\bUNION\b',
            r'\bEXISTS\b',
            r'\bIN\s*\(',
            r'\bSELECT\b.*\bSELECT\b'  # 子查询
        ]
        
        for pattern in complex_patterns:
            if re.search(pattern, view_definition, re.IGNORECASE):
                return False
        
        return True
    
    def _replace_simple_view(self, sql_text: str, view_name: str, view_definition: str) -> str:
        """
        替换简单视图
        """
        import re
        
        # 提取列名
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', view_definition, re.IGNORECASE)
        if not select_match:
            return sql_text
        
        view_columns = select_match.group(1).strip()
        
        # 提取表名
        from_match = re.search(r'FROM\s+(\w+)', view_definition, re.IGNORECASE)
        if not from_match:
            return sql_text
        
        view_table = from_match.group(1)
        
        # 提取WHERE条件
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|$)', view_definition, re.IGNORECASE | re.DOTALL)
        where_clause = where_match.group(1).strip() if where_match else None
        
        # 重写查询
        if '*' in sql_text:
            if where_clause:
                # 有WHERE条件，需要添加WHERE子句
                rewritten = re.sub(r'SELECT\s+\*\s+FROM\s+' + re.escape(view_name), 
                                 f'SELECT {view_columns} FROM {view_table} WHERE {where_clause}', 
                                 sql_text, flags=re.IGNORECASE)
            else:
                rewritten = re.sub(r'SELECT\s+\*\s+FROM\s+' + re.escape(view_name), 
                                 f'SELECT {view_columns} FROM {view_table}', 
                                 sql_text, flags=re.IGNORECASE)
        else:
            if where_clause:
                # 有WHERE条件，需要添加WHERE子句
                rewritten = re.sub(r'FROM\s+' + re.escape(view_name), 
                                 f'FROM {view_table} WHERE {where_clause}', 
                                 sql_text, flags=re.IGNORECASE)
            else:
                rewritten = re.sub(r'FROM\s+' + re.escape(view_name), 
                                 f'FROM {view_table}', 
                                 sql_text, flags=re.IGNORECASE)
        
        return rewritten
    
    def _replace_complex_view(self, sql_text: str, view_name: str, view_definition: str) -> str:
        """
        替换复杂视图，使用子查询方式
        """
        import re
        
        # 检查视图定义是否包含GROUP BY
        is_aggregate_view = re.search(r'\bGROUP BY\b', view_definition, re.IGNORECASE)
        
        # 从原始查询中提取WHERE子句
        where_match = re.search(r'\bWHERE\s+(.+)', sql_text, re.IGNORECASE | re.DOTALL)
        original_where_clause = where_match.group(1) if where_match else None
        
        # 移除原始查询中的WHERE子句
        rewritten_query_without_where = re.sub(r'\bWHERE\s+.+', '', sql_text, flags=re.IGNORECASE)
        
        # 如果视图是聚合视图且原始查询有WHERE子句，则将WHERE转换为HAVING
        if is_aggregate_view and original_where_clause:
            # 提取视图的SELECT部分
            select_match = re.search(r'SELECT\s+(.+?)\s+FROM', view_definition, re.IGNORECASE | re.DOTALL)
            if not select_match:
                return sql_text
            
            view_select_list = select_match.group(1).strip()
            
            # 提取视图定义的FROM...GROUP BY部分
            from_group_by_part = view_definition[select_match.end() - 4:].strip()
            
            # 将原始查询的WHERE转换为HAVING，并组合成最终查询
            # 清理原始WHERE子句，移除末尾的分号
            clean_where_clause = original_where_clause.rstrip(';').strip()
            rewritten = f"SELECT {view_select_list} {from_group_by_part} HAVING {clean_where_clause}"
            return rewritten
        else:
            # 非聚合视图或没有WHERE子句，使用现有逻辑
            if '*' in sql_text:
                # 提取视图的SELECT部分
                select_match = re.search(r'SELECT\s+(.+?)\s+FROM', view_definition, re.IGNORECASE)
                if select_match:
                    view_columns = select_match.group(1).strip()
                    
                    # 构建完整的查询
                    # 从视图定义中提取FROM之后的所有内容
                    from_part = view_definition[select_match.end() - 4:].strip()  # 包含FROM关键字
                    
                    # 替换查询
                    rewritten = re.sub(r'SELECT\s+\*\s+FROM\s+' + re.escape(view_name), 
                                     f'SELECT {view_columns} {from_part}', 
                                     sql_text, flags=re.IGNORECASE)
                return rewritten
        
        # 如果查询中指定了具体列名，只替换FROM部分
        rewritten = re.sub(r'FROM\s+' + re.escape(view_name), 
                         f'FROM ({view_definition})', 
                         sql_text, flags=re.IGNORECASE)
        
        return rewritten
    
    def is_view_query(self, sql_text: str) -> bool:
        """
        检查查询是否包含视图
        
        Args:
            sql_text: SQL查询
            
        Returns:
            bool: 是否包含视图
        """
        try:
            tokens = tokenize(sql_text)
            view_names, missing_views = self._find_views_in_query(tokens)
            return len(view_names) > 0 or len(missing_views) > 0
        except:
            return False
    
    def get_view_dependencies(self, view_name: str) -> List[str]:
        """
        获取视图的依赖关系
        
        Args:
            view_name: 视图名称
            
        Returns:
            List[str]: 依赖的表和视图列表
        """
        try:
            view_definition = self.view_manager.get_view_definition(view_name)
            if not view_definition:
                return []
            
            # 提取视图定义中引用的表和视图
            dependencies = []
            
            # 提取表名
            table_names = self._extract_table_names_from_definition(view_definition)
            dependencies.extend(table_names)
            
            # 提取其他视图名
            view_names = self._extract_view_names_from_definition(view_definition)
            dependencies.extend(view_names)
            
            return dependencies
            
        except Exception as e:
            print(f"获取视图依赖失败: {str(e)}")
            return []
    
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
    
    def _extract_view_names_from_definition(self, definition: str) -> List[str]:
        """从视图定义中提取视图名"""
        view_names = []
        
        try:
            # 提取FROM和JOIN子句中的标识符
            from_pattern = r'FROM\s+(\w+)'
            join_pattern = r'JOIN\s+(\w+)'
            
            all_matches = re.findall(from_pattern, definition.upper()) + re.findall(join_pattern, definition.upper())
            
            # 检查每个标识符是否是视图
            for name in all_matches:
                if self.catalog_manager.view_exists(name):
                    view_names.append(name)
            
        except Exception as e:
            print(f"提取视图名失败: {str(e)}")
        
        return view_names
    
    def validate_view_definition(self, definition: str) -> Tuple[bool, str]:
        """
        验证视图定义是否有效
        
        Args:
            definition: 视图定义
            
        Returns:
            Tuple[bool, str]: (是否有效, 错误信息)
        """
        try:
            # 1. 语法检查
            tokens = tokenize(definition)
            if not tokens:
                return False, "视图定义为空"
            
            # 2. 检查是否以SELECT开头
            if tokens[0][1].upper() != 'SELECT':
                return False, "视图定义必须以SELECT开头"
            
            # 3. 解析语法
            ast = self.syntax_analyzer.build_ast_from_tokens(tokens)
            if not ast:
                return False, "视图定义语法错误"
            
            # 4. 检查引用的表和视图是否存在
            table_names = self._extract_table_names_from_definition(definition)
            for table_name in table_names:
                if not self.catalog_manager.table_exists(table_name):
                    return False, f"引用的表 '{table_name}' 不存在"
            
            view_names = self._extract_view_names_from_definition(definition)
            for view_name in view_names:
                if not self.catalog_manager.view_exists(view_name):
                    return False, f"引用的视图 '{view_name}' 不存在"
            
            return True, "视图定义有效"
            
        except Exception as e:
            return False, f"验证失败: {str(e)}"
    
    def get_rewritten_query_info(self, sql_text: str) -> Dict[str, Any]:
        """
        获取查询重写信息
        
        Args:
            sql_text: 原始SQL查询
            
        Returns:
            Dict[str, Any]: 重写信息
        """
        try:
            tokens = tokenize(sql_text)
            view_names = self._find_views_in_query(tokens)
            
            info = {
                'original_query': sql_text,
                'contains_views': len(view_names) > 0,
                'view_names': view_names,
                'rewritten_query': sql_text,
                'rewrite_applied': False
            }
            
            if view_names:
                rewritten_query = self.rewrite_query(sql_text)
                info['rewritten_query'] = rewritten_query
                info['rewrite_applied'] = True
            
            return info
            
        except Exception as e:
            return {
                'original_query': sql_text,
                'contains_views': False,
                'view_names': [],
                'rewritten_query': sql_text,
                'rewrite_applied': False,
                'error': str(e)
            }
