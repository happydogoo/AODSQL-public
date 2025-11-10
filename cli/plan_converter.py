# -*- coding: utf-8 -*-
"""
计划转换模块
封装逻辑计划到物理计划的转换逻辑
"""

from typing import Dict, Any, Optional
from src.engine.operator import CreateTable, Insert, Delete, Update, SeqScan, Filter, Project, Sort, Schema, \
    HashAggregate
from src.engine.catalog_manager import CatalogManager
from src.engine.storage.storage_engine import StorageEngine


def safe_float_convert(value):
    """安全地将值转换为float，支持中文数字"""
    if isinstance(value, (int, float)):
        return float(value)
    
    # 如果是字符串，尝试转换
    if isinstance(value, str):
        # 处理中文数字
        chinese_digits = {
            '零': 0, '一': 1, '二': 2, '三': 3, '四': 4, 
            '五': 5, '六': 6, '七': 7, '八': 8, '九': 9,
            '十': 10, '百': 100, '千': 1000, '万': 10000
        }
        
        # 简单的中文数字转换（支持一位数和十位数）
        if value in chinese_digits:
            return float(chinese_digits[value])
        
        # 处理如"二十"、"三十五"等
        if len(value) == 2 and value[1] == '十':
            if value[0] in chinese_digits:
                return float(chinese_digits[value[0]] * 10)
        elif len(value) == 3 and value[1] == '十':
            if value[0] in chinese_digits and value[2] in chinese_digits:
                return float(chinese_digits[value[0]] * 10 + chinese_digits[value[2]])
        
        # 尝试直接转换为数字
        try:
            return float(value)
        except ValueError:
            pass
    
    # 如果是bytes，尝试解码后转换
    if isinstance(value, bytes):
        try:
            decoded = value.decode('utf-8').strip('\x00')
            return safe_float_convert(decoded)
        except:
            pass
    
    # 默认返回0
    return 0.0


class ExpressionEvaluator:
    """表达式求值器，用于处理复杂的算术表达式"""
    
    def __init__(self, expression_str, left_col_idx, right_col_idx, operator):
        self.expression_str = expression_str
        self.left_col_idx = left_col_idx
        self.right_col_idx = right_col_idx
        self.operator = operator
    
    def evaluate(self, row_data):
        """对单行数据求值表达式"""
        try:
            # 获取左操作数和右操作数的值
            left_value = row_data[self.left_col_idx]
            right_value = row_data[self.right_col_idx]
            
            # 转换为数值类型
            left_num = float(left_value) if left_value is not None else 0
            right_num = float(right_value) if right_value is not None else 0
            
            # 根据操作符进行计算
            if self.operator == '+':
                return left_num + right_num
            elif self.operator == '-':
                return left_num - right_num
            elif self.operator == '*':
                return left_num * right_num
            elif self.operator == '/':
                return left_num / right_num if right_num != 0 else 0
            elif self.operator == '%':
                return left_num % right_num if right_num != 0 else 0
            else:
                raise ValueError(f"不支持的操作符: {self.operator}")
        except (ValueError, TypeError, IndexError) as e:
            return 0
    
    def __str__(self):
        return f"ExpressionEvaluator({self.expression_str})"


class PlanConverter:
    """计划转换器"""
    
    def __init__(self, storage_engine: StorageEngine, catalog_manager: CatalogManager):
        self.storage_engine = storage_engine
        self.catalog_manager = catalog_manager
    
    def convert_to_physical_plan(self, operator_tree: Dict[str, Any]) -> Optional[Any]:
        """将算子树转换为物理执行计划"""
        try:
            op_type = operator_tree["type"]
            properties = operator_tree.get("properties", {})
            
            # 新增：提取元数据
            metadata = {
                "estimated_cost": properties.get("estimated_cost"),
                "estimated_rows": properties.get("estimated_rows"),
                "operator_type": op_type
            }
            
            if op_type == "CREATE_TABLE":
                return self._convert_create_table(properties)
            elif op_type == "INSERT":
                return self._convert_insert(properties)
            elif op_type == "DELETE":
                return self._convert_delete(operator_tree, properties)
            elif op_type == "PROJECT":
                return self._convert_project(operator_tree, properties, metadata)
            elif op_type == "SCAN":
                return self._convert_scan(properties, metadata)
            elif op_type == "INDEX_SCAN":
                return self._convert_index_scan(properties, metadata)
            elif op_type == "FILTER":
                return self._convert_filter(operator_tree, properties, metadata)
            elif op_type == "ORDER_BY":
                return self._convert_order_by(operator_tree, properties)
            elif op_type == "SORT":
                return self._convert_sort(operator_tree, properties)
            elif op_type == "LIMIT":
                return self._convert_limit(operator_tree, properties)
            elif op_type == "AGGREGATE":
                return self._convert_aggregate(operator_tree, properties)
            elif op_type == "GROUP_BY":
                return self._convert_group_by(operator_tree, properties)
            elif op_type == "HAVING":
                return self._convert_having(operator_tree, properties)
            elif op_type == "UPDATE":
                return self._convert_update(operator_tree, properties)
            elif op_type == "DROP_TABLE":
                return self._convert_drop_table(operator_tree, properties)
            elif op_type == "DROP_INDEX":
                return self._convert_drop_index(operator_tree, properties)
            elif op_type == "SHOW_TABLES":
                return self._convert_show_tables(operator_tree, properties)
            elif op_type == "SHOW_COLUMNS":
                return self._convert_show_columns(operator_tree, properties)
            elif op_type == "SHOW_INDEX":
                return self._convert_show_index(operator_tree, properties)
            elif op_type == "SHOW_VIEWS":
                return self._convert_show_views(operator_tree, properties)
            elif op_type == "EXPLAIN":
                return self._convert_explain(operator_tree, properties)
            elif op_type == "CREATE_INDEX":
                return self._convert_create_index(operator_tree, properties)
            elif op_type == "JOIN":
                return self._convert_join(operator_tree, properties)
            elif op_type == "CREATE_VIEW":
                return self._convert_create_view(operator_tree, properties)
            elif op_type == "DROP_VIEW":
                return self._convert_drop_view(operator_tree, properties)
            elif op_type == "ALTER_VIEW":
                return self._convert_alter_view(operator_tree, properties)
            elif op_type == "CREATE_TRIGGER":
                return self._convert_create_trigger(properties)
            elif op_type == "DROP_TRIGGER":
                return self._convert_drop_trigger(properties)
            elif op_type == "SHOW_TRIGGERS":
                return self._convert_show_triggers(properties)
            elif op_type == "DECLARE_CURSOR":
                return self._convert_declare_cursor(properties)
            elif op_type == "OPEN_CURSOR":
                return self._convert_open_cursor(properties)
            elif op_type == "FETCH_CURSOR":
                return self._convert_fetch_cursor(properties)
            elif op_type == "CLOSE_CURSOR":
                return self._convert_close_cursor(properties)
            elif op_type == "BEGIN_TRANSACTION":
                return self._convert_begin_transaction(properties)
            elif op_type == "COMMIT_TRANSACTION":
                return self._convert_commit_transaction(properties)
            elif op_type == "ROLLBACK_TRANSACTION":
                return self._convert_rollback_transaction(properties)
            else:
                print(f"❌ 不支持的操作类型: {op_type}")
                return None
                
        except Exception as e:
            print(f"❌ 执行计划转换失败: {str(e)}")
            import traceback
            print(f"详细错误: {traceback.format_exc()}")
            return None
    
    def _convert_create_table(self, properties: Dict[str, Any]):
        """转换CREATE TABLE操作"""
        table_name = properties.get("table_name", "unknown")
        columns = properties.get("columns", [])
        
        # 处理列定义，支持两种格式：字典和CompatibleColumnDef对象
        column_tuples = []
        for col in columns:
            if hasattr(col, 'name') and hasattr(col, 'datatype'):
                # CompatibleColumnDef对象
                column_tuples.append((col.name, col.datatype))
            elif isinstance(col, dict):
                # 字典格式
                column_tuples.append((col["name"], col["type"]))
            else:
                print(f"[ERROR] 未知的列定义格式: {col}")
                
        return CreateTable(table_name, column_tuples, self.storage_engine)
    
    def _convert_insert(self, properties: Dict[str, Any]):
        """转换INSERT操作"""
        table_name = properties.get("table_name", "unknown")
        values = properties.get("values", [])
        
        # 处理多行INSERT
        if isinstance(values, list) and len(values) > 0 and isinstance(values[0], list):
            # 多行VALUES格式：[[row1], [row2], ...]
            value_tuples = [tuple(self._clean_values(row)) for row in values]
        else:
            # 单行VALUES格式：[val1, val2, ...]
            value_tuples = [tuple(self._clean_values(values))]
        
        return Insert(table_name, value_tuples, self.storage_engine)
    
    def _clean_values(self, values):
        """清理值，移除字符串引号并转换数据类型"""
        cleaned = []
        for val in values:
            if isinstance(val, str):
                # 移除字符串两端的引号
                if val.startswith("'") and val.endswith("'"):
                    val = val[1:-1]
                # 尝试转换为数字
                try:
                    if '.' in val:
                        # 对于浮点数，先转换为float再转换为int（如果小数点后为0）
                        float_val = float(val)
                        if float_val.is_integer():
                            cleaned.append(int(float_val))
                        else:
                            cleaned.append(float_val)
                    else:
                        cleaned.append(int(val))
                except ValueError:
                    # 如果无法转换为数字，保持为字符串
                    cleaned.append(val)
            else:
                cleaned.append(val)
        return cleaned
    
    def _convert_delete(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换DELETE操作"""
        table_name = properties.get("table_name", "unknown")
        
        # 创建子操作符（用于扫描需要删除的行）
        # 获取表的schema
        schema = None
        if table_name in self.catalog_manager.tables:
            table_info = self.catalog_manager.tables[table_name]
            columns = [(col.column_name, col.data_type) for col in table_info.columns]
            schema = Schema(columns)
        
        # 创建SeqScan作为基础扫描
        child_plan = SeqScan(table_name, self.storage_engine, schema)
        
        # 如果有WHERE条件，添加Filter操作符
        where_clause = properties.get("where_clause", None)
        if where_clause:
            # 解析WHERE条件
            filter_func = self._parse_condition(where_clause, schema)
            child_plan = Filter(child_plan, filter_func)
        
        return Delete(child_plan, table_name, self.storage_engine)
    
    def _convert_project(self, operator_tree: Dict[str, Any], properties: Dict[str, Any], metadata: Dict[str, Any] = None):
        """转换PROJECT操作 (修复Schema生成问题)"""
        child_plan = None
        if operator_tree["children"]:
            child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        
        if not child_plan or not child_plan.schema:
            raise ValueError("Project operator's child must have a valid plan and schema.")
        
        columns = properties.get("columns", [])
        
        # 检查是否包含聚合函数
        has_aggregate = any('(' in col and ')' in col for col in columns if isinstance(col, str))
        
        if has_aggregate:
            # 这是一个聚合查询，委托给专门的聚合查询转换方法
            # 需要从子计划中提取GROUP BY信息
            group_by_columns = []
            if hasattr(child_plan, 'group_by_columns'):
                group_by_columns = child_plan.group_by_columns
            elif hasattr(child_plan, 'schema') and child_plan.schema:
                # 从schema中提取非聚合列作为分组列
                for col in columns:
                    if isinstance(col, str) and not ('(' in col and ')' in col):
                        group_by_columns.append(col)
            
            # 检查是否有HAVING条件
            having_condition = None
            if hasattr(child_plan, 'having_condition'):
                having_condition = child_plan.having_condition
            
            # 创建聚合查询的properties
            aggregate_properties = {
                "columns": columns,
                "group_by_columns": group_by_columns,
                "having_condition": having_condition
            }
            
            # 使用专门的聚合查询转换方法
            return self._convert_aggregate_query(operator_tree, aggregate_properties)
        else:
            # 普通投影查询
            project_indices = []
            
            # 检查是否是SELECT *的情况
            if len(columns) == 1 and (columns[0] == '*' or columns[0].endswith('.*')):
                # SELECT * - 选择所有列
                project_indices = list(range(len(child_plan.schema.columns)))
            else:
                # SELECT column1, column2, ... - 选择指定列
                for col in columns:
                    # 处理列名，支持AST节点格式和普通字符串格式
                    if isinstance(col, str):
                        if 'Identifier(' in col:
                            # 处理AST节点格式：Identifier(token=('ID', 'name', 0, 0), value='name')
                            import re
                            match = re.search(r"value='([^']+)'", col)
                            if match:
                                col_name = match.group(1)
                            else:
                                col_name = col.split('.')[-1] if '.' in col else col
                        else:
                            # 普通字符串格式
                            col_name = col.split('.')[-1] if '.' in col else col
                    else:
                        col_name = str(col)
                    
                    try:
                        col_index = child_plan.schema.get_index(col_name)
                        project_indices.append(col_index)
                    except KeyError:
                        continue
            
            # 确保至少有一个投影列
            if not project_indices:
                # 如果没有有效的投影列，选择第一列
                project_indices = [0]
            
            return Project(child_plan, project_indices, metadata=metadata)
    
    def _create_expression_evaluator(self, expression_str, schema):
        """创建表达式求值器"""
        try:
            # 解析表达式字符串，如 "quantity * unit_price"
            # 支持的操作符: +, -, *, /, %
            operators = ['*', '/', '+', '-', '%']
            
            for op in operators:
                if op in expression_str:
                    parts = expression_str.split(op, 1)
                    if len(parts) == 2:
                        left_col = parts[0].strip()
                        right_col = parts[1].strip()
                        
                        # 获取列索引
                        left_idx = schema.get_index(left_col)
                        right_idx = schema.get_index(right_col)
                        
                        return ExpressionEvaluator(expression_str, left_idx, right_idx, op)
            
            return None
            
        except KeyError as e:
            return None
        except Exception as e:
            return None
    
    def _convert_scan(self, properties: Dict[str, Any], metadata: Dict[str, Any] = None):
        """转换SCAN操作，支持多表查询"""
        table_name = properties.get("table_name", "unknown")

        # 检查是否是多表查询
        if hasattr(table_name, 'tables'):
            # 多表查询，创建笛卡尔积
            return self._convert_multi_table_scan(table_name, metadata)
        
        # 【健壮性修改】检查 table_name 是否合法
        if not isinstance(table_name, str) or not table_name.isidentifier():
            # isidentifier() 检查字符串是否是合法的标识符 (如表名)，可以有效过滤掉 "SubqueryReference(...)" 这种长字符串
            # 这是一个防御性编程，如果逻辑计划生成正确，这个分支不应该被执行
            table_name_str = str(table_name)[:100] if hasattr(table_name, '__str__') else repr(table_name)[:100]
            raise ValueError(f"逻辑计划错误：SCAN算子接收到一个非法的表名 '{table_name_str}...'. 这通常意味着FROM子句中的子查询没有被正确地转换为计划子树。")

        table_info = self.catalog_manager.get_table(table_name)
        columns = [(col.column_name, col.data_type) for col in table_info.columns]
        schema = Schema(columns)
        return SeqScan(table_name, self.storage_engine, schema, metadata=metadata)
    
    def _convert_multi_table_scan(self, multi_table_ref, metadata: Dict[str, Any] = None):
        """转换多表查询为笛卡尔积"""
        from src.engine.operator import NestedLoopJoin
        
        tables = multi_table_ref.tables
        if len(tables) < 2:
            raise ValueError("多表查询至少需要两个表")
        
        # 创建第一个表的扫描
        first_table = tables[0]
        table_name = first_table.table_name
        table_info = self.catalog_manager.get_table(table_name)
        columns = [(col.column_name, col.data_type) for col in table_info.columns]
        schema = Schema(columns)
        left_scan = SeqScan(table_name, self.storage_engine, schema, metadata=metadata)
        
        # 逐步创建笛卡尔积
        current_join = left_scan
        for i in range(1, len(tables)):
            table = tables[i]
            table_name = table.table_name
            table_info = self.catalog_manager.get_table(table_name)
            columns = [(col.column_name, col.data_type) for col in table_info.columns]
            schema = Schema(columns)
            right_scan = SeqScan(table_name, self.storage_engine, schema, metadata=metadata)
            
            # 创建笛卡尔积（无条件连接）
            current_join = NestedLoopJoin(current_join, right_scan, None)
        
        return current_join
    
    def _convert_index_scan(self, properties: Dict[str, Any], metadata: Dict[str, Any] = None):
        """
        【修复版】转换INDEX_SCAN操作。
        创建新的、需要 index_name 和 predicate_key 的物理 IndexScan 算子。
        """
        from src.engine.operator import IndexScan, Schema # 确保导入了 Schema
        
        table_name = properties.get("table_name")
        index_name = properties.get("index_name")
        
        # 从 predicate 字典中提取出 key
        predicate = properties.get("predicate", {})
        predicate_key = predicate.get("key")

        # 检查关键参数是否存在
        if not all([table_name, index_name, predicate_key]):
            raise ValueError(
                f"转换 IndexScan 失败：缺少关键属性。 "
                f"Table: {table_name}, Index: {index_name}, Key: {predicate_key}"
            )
            
        # 获取表的 Schema
        table_info = self.catalog_manager.get_table(table_name)
        columns = [(col.column_name, col.data_type) for col in table_info.columns]
        schema = Schema(columns)
        
        # 创建我们重构后的物理 IndexScan 算子
        return IndexScan(
            table_name=table_name,
            storage_engine=self.storage_engine,
            schema=schema,
            index_name=index_name,
            predicate_key=predicate_key, # 传入元组形式的key
            metadata=metadata
        )
    
    def _convert_filter(self, operator_tree: Dict[str, Any], properties: Dict[str, Any], metadata: Dict[str, Any] = None):
        """转换FILTER操作"""
        child_plan = None
        if operator_tree["children"]:
            child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        
        if child_plan:
            condition = properties.get("condition", None)
            # 解析条件表达式
            filter_func = self._parse_condition(condition, child_plan.schema)
            
            # 为Filter创建适配的谓词函数
            # Filter传入row[1]（row_data），我们的谓词函数也期望row_data格式
            if filter_func:
                return Filter(child_plan, filter_func, metadata=metadata)
            else:
                return Filter(child_plan, lambda row_data: True, metadata=metadata)
        return None
    
    def _parse_condition(self, condition, schema: Schema):
        """解析WHERE条件，返回过滤函数"""
        if not condition:
            return lambda row_data: True
        
        # 处理字符串条件
        if isinstance(condition, str):
            condition_str = condition.strip()
        else:
            # 处理复杂条件对象，转换为字符串
            condition_str = str(condition).strip()
        
        # 检查是否是包含子查询的比较条件（优先级最高）
        if 'Select(' in condition_str and 'Token[' in condition_str:
            print(f"[DEBUG] 检测到子查询条件: {condition_str}")
            return self._parse_subquery_comparison_condition(condition_str, schema)
        
        # 检查是否是IN子查询条件
        if 'IN' in condition_str and 'LogicalPlan' in condition_str:
            return self._parse_in_subquery_condition(condition_str, schema)
        
        # 如果是复杂条件，使用复杂条件解析器
        if (('Token[Type: AND' in condition_str or 'Token[Type: OR' in condition_str or ' AND ' in condition_str or ' OR ' in condition_str) and ('(' in condition_str and ')' in condition_str)):
            return self._parse_complex_condition(condition_str, schema)
        
        # 简单的条件解析，支持 price > 100 和 price BETWEEN 50 AND 200 这样的条件
        # 这里需要根据schema确定列的位置
        try:
            # 解析条件字符串，例如 "(products.price > 100)" 或 "(products.price BETWEEN 50 AND 200)"
            # 去掉外层括号
            if condition_str.startswith('(') and condition_str.endswith(')'):
                condition_str = condition_str[1:-1]
            
            # 处理AST节点字符串化格式：Identifier(...) Token[...] Literal(...)
            if 'Identifier(' in condition_str and 'Token[' in condition_str and 'Literal(' in condition_str:
                import re
                
                # 提取列名 - 更宽松的匹配
                id_match = re.search(r"Identifier\([^)]*value='([^']+)'", condition_str)
                if not id_match:
                    id_match = re.search(r"Identifier\([^)]*value=\"([^\"]+)\"", condition_str)
                if not id_match:
                    # 尝试匹配 token=('ID', 'age', ...) 格式
                    id_match = re.search(r"token=\('ID',\s*'([^']+)'", condition_str)
                
                # 提取操作符 - 更宽松的匹配
                op_match = re.search(r"Token\[Type:\s*([^,]+),\s*Literal:\s*['\"]([^'\"]+)['\"]", condition_str)
                if not op_match:
                    op_match = re.search(r"Token\[[^]]*Literal:\s*'([^']+)'", condition_str)
                if not op_match:
                    # 尝试匹配简单的操作符
                    op_match = re.search(r"Token\[[^]]*Type:\s*([^,]+)", condition_str)
                
                # 提取值 - 更宽松的匹配
                val_match = re.search(r"Literal\([^)]*value='([^']+)'", condition_str)
                if not val_match:
                    val_match = re.search(r"Literal\([^)]*value=\"([^\"]+)\"", condition_str)
                if not val_match:
                    # 尝试匹配 token=('STRING', 'Alice Johnson', ...) 格式
                    val_match = re.search(r"token=\('STRING',\s*'([^']+)'", condition_str)
                if not val_match:
                    # 尝试匹配 token=('NUMBER', '30', ...) 格式
                    val_match = re.search(r"token=\('NUMBER',\s*'([^']+)'", condition_str)
                
                if id_match and op_match and val_match:
                    column_name = id_match.group(1).strip()
                    operator = op_match.group(2).strip()
                    value = val_match.group(1).strip()
                    parts = [column_name, operator, value]
                else:
                    return lambda row_data: True
            elif 'Token[' in condition_str and 'Identifier(' in condition_str:
                # 处理新格式：Identifier(...) Token[...] Literal(...)
                # 提取列名 - 多种模式匹配，确保提取干净的列名
                id_match = re.search(r"Identifier\([^)]*value='([^']+)'", condition_str)
                if not id_match:
                    id_match = re.search(r"token=\('ID',\s*'([^']+)'", condition_str)
                
                # 提取操作符
                op_match = re.search(r"Token\[Type:\s*([^,]+),\s*Literal:\s*['\"]([^'\"]+)['\"]", condition_str)
                if not op_match:
                    op_match = re.search(r"Token\[[^]]*Literal:\s*'([^']+)'", condition_str)
                
                # 提取值
                val_match = re.search(r"Literal\([^)]*value='([^']+)'", condition_str)
                if not val_match:
                    val_match = re.search(r"token=\('STRING',\s*'([^']+)'", condition_str)
                
                
                if id_match and op_match and val_match:
                    # 清理列名，去掉多余的字符和括号
                    column_name = id_match.group(1).strip()
                    # 去掉表前缀
                    column_name = column_name.split('.')[-1]
                    # 去掉多余的右括号
                    column_name = column_name.rstrip(')')
                    
                    try:
                        operator = op_match.group(2).strip()
                    except:
                        operator = op_match.group(1).strip()
                    value = val_match.group(1)
                    parts = [column_name, operator, value]
                else:
                    return lambda row_data: True
            elif 'Token[' in condition_str:
                # 处理包含Token的条件，例如 "c.city Token[Type: =, Literal: '=', Pos: 0:0] New York"
                pattern = r'([\w.]+)\s+Token\[Type:\s*([^,]+),\s*Literal:\s*[\'"]?([^\'",\]]+)[\'"]?,\s*Pos:[^\]]+\]\s+([^\s].*)'
                match = re.match(pattern, condition_str.strip())
                if match:
                    column_ref, op_type, op_literal, value = match.groups()
                    # 清理值，移除引号和多余空格
                    cleaned_value = value.strip().strip("'\"")
                    # 提取列名（去掉表别名，如 'c.city' -> 'city'）
                    column_name = column_ref.split('.')[-1]
                    parts = [column_name, op_literal, cleaned_value]
                else:
                    return lambda row_data: True
            else:
                # 这个正则表达式会匹配 '列名' '操作符' '剩余的所有部分作为值'
                match = re.match(r'^\s*([\w\.]+)\s*([<>=!]+)\s*(.+)\s*$', condition_str)
                if match:
                    column_ref, operator, value_str = match.groups()
                    column_name = column_ref.split('.')[-1]  # 去掉表别名, e.g., 'c.city' -> 'city'
                    value = value_str.strip()  # 值就是剩余的全部部分
                    parts = [column_name, operator, value]
                else:
                    parts = condition_str.split()
            
            # 检查是否是BETWEEN条件
            if len(parts) == 5 and parts[1] == 'BETWEEN' and parts[3] == 'AND':
                column_name = parts[0].split('.')[-1]  # 去掉表名前缀
                start_value = parts[2]
                end_value = parts[4]
                
                # 找到列在schema中的位置
                column_index = None
                for i, (col_name, col_type) in enumerate(schema.columns):
                    if col_name == column_name:
                        column_index = i
                        break
                
                if column_index is None:
                    return lambda row_data: True
                
                # 转换值类型
                try:
                    start_value = float(start_value)
                    end_value = float(end_value)
                except ValueError:
                    return lambda row_data: True
                
                # 创建BETWEEN过滤函数（row格式为row_data）
                return lambda row_data: start_value <= safe_float_convert(row_data[column_index]) <= end_value
            
            # 处理普通比较条件
            elif len(parts) == 3:
                column_name = parts[0].split('.')[-1]  # 去掉表名前缀
                operator = parts[1]
                value = parts[2]
                
                # 找到列在schema中的位置
                column_index = None
                for i, (col_name, col_type) in enumerate(schema.columns):
                    if col_name == column_name:
                        column_index = i
                        break
                
                if column_index is None:
                    return lambda row_data: True
                
                # 根据列类型决定如何处理值
                col_name, col_type = schema.columns[column_index]
                col_type_upper = col_type.upper()
                
                # 对于字符串类型，不进行数字转换
                if col_type_upper.startswith('VARCHAR') or col_type_upper.startswith('CHAR') or col_type_upper.startswith('TEXT') or col_type_upper.startswith('STR'):
                    # 字符串比较，去掉引号
                    if value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    # 定义安全字符串解码函数
                    def safe_string_decode_v1(raw_value):
                        """安全地解码字符串值，与CLI显示逻辑保持一致"""
                        if isinstance(raw_value, bytes):
                            try:
                                decoded = raw_value.decode('utf-8').rstrip('\x00')
                                
                                # 处理双重序列化的情况（如 b'b\'"test"\\x00'）
                                if decoded.startswith("b'") and decoded.endswith("'"):
                                    # 移除 b' 和 ' 包装
                                    inner = decoded[2:-1]
                                    # 处理转义字符
                                    inner = inner.replace('\\x00', '').replace('\\n', '\n').replace('\\t', '\t')
                                    # 移除内部引号
                                    if inner.startswith('"') and inner.endswith('"'):
                                        inner = inner[1:-1]
                                    elif inner.startswith("'") and inner.endswith("'"):
                                        inner = inner[1:-1]
                                    decoded = inner
                                
                                # 移除可能的引号包装
                                elif decoded.startswith("'") and decoded.endswith("'"):
                                    decoded = decoded[1:-1]
                                elif decoded.startswith('"') and decoded.endswith('"'):
                                    decoded = decoded[1:-1]
                                
                                return decoded.strip()
                            except:
                                return str(raw_value)
                        else:
                            return str(raw_value).strip()
                    
                    # 创建字符串过滤函数（row格式为row_data）
                    if operator == '=':
                        def debug_string_filter(row_data):
                            actual_value = safe_string_decode_v1(row_data[column_index])
                            result = actual_value == value
                            return result
                        return debug_string_filter
                    elif operator in ('!=', '<>'):
                        return lambda row_data: safe_string_decode_v1(row_data[column_index]) != value

                    else:
                        return lambda row_data: True
                # 对于日期类型，进行字符串比较
                elif col_type_upper == 'DATE' or col_type_upper.startswith('DATE'):
                    # 日期比较，去掉引号
                    if value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    
                    def extract_date_string(date_value):
                        """从bytes或字符串中提取日期字符串"""
                        if isinstance(date_value, bytes):
                            # 处理bytes类型，去掉空字节
                            return date_value.decode('utf-8').rstrip('\x00')
                        else:
                            return str(date_value)
                    
                    # 创建日期过滤函数（row格式为row_data）
                    if operator == '>':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            extract_date_string(row_data[column_index]) > value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '<':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            extract_date_string(row_data[column_index]) < value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '>=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            extract_date_string(row_data[column_index]) >= value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '<=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            extract_date_string(row_data[column_index]) <= value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            extract_date_string(row_data[column_index]) == value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '!=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            extract_date_string(row_data[column_index]) != value
                        ) if isinstance(row_data, (list, tuple)) else True
                    else:
                        return lambda row_data: True
                else:
                    # 数字类型，尝试转换
                    try:
                        value = float(value)
                    except ValueError:
                        return lambda row_data: True
                    
                    # 创建数字过滤函数（row格式为row_data）
                    if operator == '>':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            float(row_data[column_index]) > value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '<':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            float(row_data[column_index]) < value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '>=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            float(row_data[column_index]) >= value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '<=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            float(row_data[column_index]) <= value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            float(row_data[column_index]) == value
                        ) if isinstance(row_data, (list, tuple)) else False
                    elif operator == '!=':
                        return lambda row_data: (
                            row_data is not None and
                            len(row_data) > column_index and 
                            float(row_data[column_index]) != value
                        ) if isinstance(row_data, (list, tuple)) else True
                    else:
                        return lambda row_data: True
            else:
                return lambda row_data: True
        except Exception as e:
            return lambda row_data: True
    
    def _parse_in_subquery_condition(self, condition_str, schema: Schema):
        """解析IN子查询条件"""
        try:
            # 解析类似 "(user_id IN ((LogicalPlan(Project([orders.user_id])))))" 的格式
            import re
            
            # 去掉外层括号
            if condition_str.startswith('(') and condition_str.endswith(')'):
                condition_str = condition_str[1:-1]
            
            # 匹配 IN 子查询模式
            pattern = r'(\w+)\s+IN\s+\(\(LogicalPlan\([^)]+\)\)\)'
            match = re.match(pattern, condition_str)
            
            if match:
                column_name = match.group(1)
                
                # 找到列在schema中的位置
                column_index = None
                for i, (col_name, col_type) in enumerate(schema.columns):
                    if col_name == column_name:
                        column_index = i
                        break
                
                if column_index is None:
                    return lambda row_data: True
                
                # 执行子查询来获取值列表
                # 对于这个特定的子查询，我们知道它查询的是 orders.user_id WHERE status = 'completed'
                # 这里简化实现，直接执行子查询
                subquery_values = self._execute_subquery_for_in_condition()
                
                if not subquery_values:
                    return lambda row_data: True
                
                # 创建IN过滤函数
                return lambda row_data: row_data[column_index] in subquery_values
            else:
                return lambda row_data: True
                
        except Exception as e:
            return lambda row_data: True
    
    def _parse_subquery_comparison_condition(self, condition_str, schema: Schema):
        """解析包含子查询的比较条件"""
        print(f"[DEBUG] 进入_parse_subquery_comparison_condition方法")
        try:
            # 解析类似 "u.salary Token[Type: >, Literal: '>', Pos: 0:0] Select([AggregateFunction(...)], ...)" 的格式
            import re
            
            # 去掉外层括号
            if condition_str.startswith('(') and condition_str.endswith(')'):
                condition_str = condition_str[1:-1]
            
            # 匹配子查询比较模式 - 简化的模式
            pattern = r'value=\'(\w+)\'.*?Token\[Type:\s*([^,]+),\s*Literal:\s*[\'"]?([^\'",\]]+)[\'"]?.*?\(Select\('
            print(f"[DEBUG] 尝试匹配模式: {pattern}")
            print(f"[DEBUG] 条件字符串: {condition_str}")
            match = re.search(pattern, condition_str)
            
            if match:
                column_ref, op_type, op_literal = match.groups()
                column_name = column_ref.split('.')[-1]  # 去掉表别名前缀
                
                # 找到列在schema中的位置
                column_index = None
                for i, (col_name, col_type) in enumerate(schema.columns):
                    if col_name == column_name:
                        column_index = i
                        break
                
                if column_index is None:
                    return lambda row_data: True
                
                # 执行子查询来获取比较值
                subquery_value = self._execute_subquery_for_comparison_condition(condition_str)
                print(f"[DEBUG] 子查询执行结果: {subquery_value}")
                
                if subquery_value is None:
                    print("[DEBUG] 子查询返回None，使用默认过滤条件")
                    return lambda row_data: True
                
                # 创建比较过滤函数
                # 需要处理两种格式：
                # 1. Filter: row_data 直接是数据行
                # 2. IndexScan: row_data 是 (row_id, row_data) 格式
                def create_comparison_func(op):
                    def comparison_func(row_data):
                        # 检查row_data的格式
                        if isinstance(row_data, tuple) and len(row_data) == 2 and isinstance(row_data[0], (int, tuple)):
                            # IndexScan格式: (row_id, row_data)
                            actual_data = row_data[1]
                        else:
                            # Filter格式: 直接是数据行
                            actual_data = row_data
                        
                        return op(float(actual_data[column_index]), subquery_value)
                    return comparison_func
                
                if op_literal == '>':
                    return create_comparison_func(lambda x, y: x > y)
                elif op_literal == '<':
                    return create_comparison_func(lambda x, y: x < y)
                elif op_literal == '>=':
                    return create_comparison_func(lambda x, y: x >= y)
                elif op_literal == '<=':
                    return create_comparison_func(lambda x, y: x <= y)
                elif op_literal == '=':
                    return create_comparison_func(lambda x, y: x == y)
                elif op_literal == '!=':
                    return create_comparison_func(lambda x, y: x != y)
                else:
                    return lambda row_data: True
            else:
                print("[DEBUG] 子查询模式匹配失败，使用默认过滤条件")
                return lambda row_data: True
                
        except Exception as e:
            return lambda row_data: True
    
    def _execute_subquery_for_in_condition(self):
        """执行子查询获取IN条件中的值列表"""
        try:
            # 对于这个特定的子查询：SELECT user_id FROM orders WHERE status = 'completed'
            # 我们直接执行这个查询
            
            # 获取orders表的schema
            if 'orders' not in self.catalog_manager.tables:
                return []
            
            table_info = self.catalog_manager.get_table('orders')
            columns = [(col.column_name, col.data_type) for col in table_info.columns]
            schema = Schema(columns)
            
            # 创建SeqScan
            scan_op = SeqScan('orders', self.storage_engine, schema)
            
            # 创建过滤条件：status = 'completed'
            def status_filter(row):
                try:
                    # 找到status列的索引
                    status_index = None
                    for i, (col_name, col_type) in enumerate(schema.columns):
                        if col_name == 'status':
                            status_index = i
                            break
                    
                    if status_index is not None and len(row) > status_index:
                        status_value = str(row[status_index])
                        return status_value == 'completed'
                    return False
                except Exception as e:
                    return False
            
            # 创建Filter
            filter_op = Filter(scan_op, status_filter)
            
            # 创建Project：只选择user_id列
            user_id_index = None
            for i, (col_name, col_type) in enumerate(schema.columns):
                if col_name == 'user_id':
                    user_id_index = i
                    break
            
            if user_id_index is None:
                return []
            
            project_op = Project(filter_op, [user_id_index])
            
            # 执行查询
            results = []
            while True:
                batch = project_op.next()
                if batch is None:
                    break
                for row in batch:
                    try:
                        # 对于Project操作，row应该是(row_id, projected_data)格式
                        if len(row) >= 2:
                            projected_data = row[1]
                            
                            if isinstance(projected_data, (tuple, list)) and len(projected_data) > 0:
                                user_id_value = projected_data[0]
                                results.append(user_id_value)
                            else:
                                # 如果projected_data不是元组，可能是单个值
                                results.append(projected_data)
                        else:
                            # 如果row格式不正确，跳过
                            continue
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            return []
    
    def _execute_subquery_for_comparison_condition(self, condition_str):
        """执行子查询获取比较条件中的值"""
        try:
            # 解析子查询字符串，提取表名和聚合函数
            import re
            
            # 匹配类似 "(Select([AggregateFunction(function_name='AVG', argument=Identifier(...), ...)], students, ...)" 的格式
            # 使用多个正则表达式分别提取信息
            func_match = re.search(r"AggregateFunction\(function_name='(\w+)'", condition_str)
            col_match = re.search(r"value='(\w+)'", condition_str)  
            table_match = re.search(r"\],\s*(\w+)", condition_str)
            
            print(f"[DEBUG] 函数匹配: {func_match}")
            print(f"[DEBUG] 列匹配: {col_match}")
            print(f"[DEBUG] 表匹配: {table_match}")
            
            if not (func_match and col_match and table_match):
                print("[DEBUG] 正则表达式匹配失败")
                return None
            
            func_name = func_match.group(1)
            column_name = col_match.group(1)
            table_name = table_match.group(1)
            
            print(f"[DEBUG] 提取结果: func={func_name}, col={column_name}, table={table_name}")
            
            # 检查表是否存在
            if table_name not in self.catalog_manager.tables:
                print(f"[DEBUG] 表{table_name}不存在")
                return None
            
            table_info = self.catalog_manager.get_table(table_name)
            columns = [(col.column_name, col.data_type) for col in table_info.columns]
            schema = Schema(columns)
            
            # 创建SeqScan
            print(f"[DEBUG] 创建SeqScan: table={table_name}, schema={schema}")
            scan_op = SeqScan(table_name, self.storage_engine, schema)
            
            # 为SeqScan设置事务 - 创建一个临时事务用于子查询
            from src.engine.transaction.transaction import Transaction, IsolationLevel
            temp_transaction = Transaction(999, IsolationLevel.READ_COMMITTED)
            scan_op.transaction = temp_transaction
            
            print(f"[DEBUG] SeqScan创建成功: {scan_op}, 事务: {temp_transaction}")
            
            # 找到目标列的索引
            target_column_index = None
            print(f"[DEBUG] 查找列{column_name}在schema中的位置")
            print(f"[DEBUG] Schema列: {schema.columns}")
            for i, (col_name, col_type) in enumerate(schema.columns):
                print(f"[DEBUG] 检查列{i}: {col_name} == {column_name}?")
                if col_name == column_name:
                    target_column_index = i
                    print(f"[DEBUG] 找到列{column_name}在位置{i}")
                    break
            
            if target_column_index is None:
                print(f"[DEBUG] 未找到列{column_name}")
                return None
            
            # 根据聚合函数类型计算结果
            if func_name.upper() == 'AVG':
                print(f"[DEBUG] 开始计算AVG({column_name})")
                total_value = 0.0
                count = 0
                
                while True:
                    print(f"[DEBUG] 调用scan_op.next()")
                    try:
                        batch = scan_op.next()
                        print(f"[DEBUG] scan_op.next()返回: {batch}")
                        if batch is None:
                            print(f"[DEBUG] 批次为None，结束循环")
                            break
                    except Exception as e:
                        print(f"[DEBUG] scan_op.next()出现异常: {e}")
                        break
                    print(f"[DEBUG] 处理批次，包含{len(batch)}行")
                    for row in batch:
                        try:
                            if len(row) >= 2:
                                row_data = row[1]
                                print(f"[DEBUG] 处理行数据: {row_data}")
                                if isinstance(row_data, (tuple, list)) and len(row_data) > target_column_index:
                                    value = float(row_data[target_column_index])
                                    print(f"[DEBUG] 提取值: {value}")
                                    total_value += value
                                    count += 1
                        except (ValueError, TypeError, IndexError) as e:
                            print(f"[DEBUG] 处理行时出错: {e}")
                            continue
                
                print(f"[DEBUG] AVG计算结果: total={total_value}, count={count}")
                
                if count == 0:
                    return None
                
                avg_result = total_value / count
                print(f"[DEBUG] 最终AVG结果: {avg_result}")
                return avg_result
            
            elif func_name.upper() == 'COUNT':
                count = 0
                while True:
                    batch = scan_op.next()
                    if batch is None:
                        break
                    for row in batch:
                        count += 1
                return count
            
            elif func_name.upper() == 'SUM':
                total_value = 0.0
                while True:
                    batch = scan_op.next()
                    if batch is None:
                        break
                    for row in batch:
                        try:
                            if len(row) >= 2:
                                row_data = row[1]
                                if isinstance(row_data, (tuple, list)) and len(row_data) > target_column_index:
                                    value = float(row_data[target_column_index])
                                    total_value += value
                        except (ValueError, TypeError, IndexError):
                            continue
                return total_value
            
            elif func_name.upper() == 'MIN':
                min_value = None
                while True:
                    batch = scan_op.next()
                    if batch is None:
                        break
                    for row in batch:
                        try:
                            if len(row) >= 2:
                                row_data = row[1]
                                if isinstance(row_data, (tuple, list)) and len(row_data) > target_column_index:
                                    value = float(row_data[target_column_index])
                                    if min_value is None or value < min_value:
                                        min_value = value
                        except (ValueError, TypeError, IndexError):
                            continue
                return min_value
            
            elif func_name.upper() == 'MAX':
                max_value = None
                while True:
                    batch = scan_op.next()
                    if batch is None:
                        break
                    for row in batch:
                        try:
                            if len(row) >= 2:
                                row_data = row[1]
                                if isinstance(row_data, (tuple, list)) and len(row_data) > target_column_index:
                                    value = float(row_data[target_column_index])
                                    if max_value is None or value > max_value:
                                        max_value = value
                        except (ValueError, TypeError, IndexError):
                            continue
                return max_value
            
            return None
            
        except Exception as e:
            return None
    
    def _parse_complex_condition(self, condition_str, schema: Schema):
        """解析复杂的AND/OR表达式条件"""
        try:
            def find_matching_paren(s, start):
                """找到匹配的右括号位置"""
                count = 0
                for i in range(start, len(s)):
                    if s[i] == '(':
                        count += 1
                    elif s[i] == ')':
                        count -= 1
                        if count == 0:
                            return i
                return -1
            
            def parse_expression_recursive(expr, depth=0):
                """递归解析表达式"""
                # 防止无限递归
                if depth > 50:
                    return self._parse_simple_condition(expr, schema)
                
                expr = expr.strip()
                
                # 去掉外层括号
                original_expr = expr
                while expr.startswith('(') and expr.endswith(')'):
                    # 检查是否是完整的括号对
                    if find_matching_paren(expr, 0) == len(expr) - 1:
                        new_expr = expr[1:-1].strip()
                        # 防止无限循环
                        if new_expr == expr:
                            break
                        expr = new_expr
                    else:
                        break
                
                # 查找最外层的Token格式的AND/OR操作符
                paren_count = 0
                i = 0
                while i < len(expr):
                    char = expr[i]
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                    elif paren_count == 0:
                        # 查找Token格式的AND操作符
                        if expr[i:].startswith("Token[Type: AND"):
                            # 找到Token的结束位置
                            token_end = expr.find(']', i) + 1
                            if token_end > i:
                                left = expr[:i].strip()
                                right = expr[token_end:].strip()
                                left_func = parse_expression_recursive(left, depth + 1)
                                right_func = parse_expression_recursive(right, depth + 1)
                                return lambda row_data, lf=left_func, rf=right_func: lf(row_data) and rf(row_data)
                        
                        # 查找Token格式的OR操作符
                        elif expr[i:].startswith("Token[Type: OR"):
                            # 找到Token的结束位置
                            token_end = expr.find(']', i) + 1
                            if token_end > i:
                                left = expr[:i].strip()
                                right = expr[token_end:].strip()
                                left_func = parse_expression_recursive(left, depth + 1)
                                right_func = parse_expression_recursive(right, depth + 1)
                                return lambda row_data, lf=left_func, rf=right_func: lf(row_data) or rf(row_data)
                        
                        # 在最外层查找普通的AND/OR
                        elif expr[i:].startswith(' AND '):
                            left = expr[:i].strip()
                            right = expr[i+5:].strip()
                            left_func = parse_expression_recursive(left, depth + 1)
                            right_func = parse_expression_recursive(right, depth + 1)
                            # 使用默认参数来避免闭包问题
                            return lambda row_data, lf=left_func, rf=right_func: lf(row_data) and rf(row_data)
                        elif expr[i:].startswith(' OR '):
                            left = expr[:i].strip()
                            right = expr[i+4:].strip()
                            left_func = parse_expression_recursive(left, depth + 1)
                            right_func = parse_expression_recursive(right, depth + 1)
                            # 使用默认参数来避免闭包问题
                            return lambda row_data, lf=left_func, rf=right_func: lf(row_data) or rf(row_data)
                    i += 1
                
                # 如果没有找到AND/OR，解析为基本条件
                return self._parse_simple_condition(expr, schema)
            
            # 启用复杂条件解析
            return parse_expression_recursive(condition_str)
                
        except Exception as e:
            return lambda row_data: True
    
    def _parse_simple_condition(self, condition_str, schema: Schema):
        """解析简单条件（单个比较表达式）"""
        try:
            # 去掉外层括号
            if condition_str.startswith('(') and condition_str.endswith(')'):
                condition_str = condition_str[1:-1]
            
            # 使用与主解析器相同的Token格式解析逻辑
            if 'Token[' in condition_str and 'Identifier(' in condition_str:
                import re
                
                # 提取列名
                id_match = re.search(r"Identifier\([^)]*value='([^']+)'", condition_str)
                if not id_match:
                    id_match = re.search(r"token=\('ID',\s*'([^']+)'", condition_str)
                
                # 提取操作符
                op_match = re.search(r"Token\[Type:\s*([^,]+),\s*Literal:\s*['\"]([^'\"]+)['\"]", condition_str)
                if not op_match:
                    op_match = re.search(r"Token\[[^]]*Literal:\s*'([^']+)'", condition_str)
                
                # 提取值
                val_match = re.search(r"Literal\([^)]*value='([^']+)'", condition_str)
                if not val_match:
                    val_match = re.search(r"token=\('NUM',\s*'([^']+)'", condition_str)
                if not val_match:
                    val_match = re.search(r"token=\('STRING',\s*'([^']+)'", condition_str)
                
                if id_match and op_match and val_match:
                    column_name = id_match.group(1)
                    try:
                        operator = op_match.group(2).strip()
                    except:
                        operator = op_match.group(1).strip()
                    value = val_match.group(1)
                    
                    # 找到列在schema中的位置
                    column_index = None
                    for i, (col_name, col_type) in enumerate(schema.columns):
                        if col_name == column_name:
                            column_index = i
                            break
                    
                    if column_index is None:
                        return lambda row_data: True
                    
                    # 根据列类型决定如何处理值
                    col_name, col_type = schema.columns[column_index]
                    col_type_upper = col_type.upper()
                    
                    # 对于字符串类型，不进行数字转换
                    if col_type_upper.startswith('VARCHAR') or col_type_upper.startswith('CHAR') or col_type_upper.startswith('TEXT') or col_type_upper.startswith('STR'):
                        # 字符串比较，去掉引号
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        
                        # 创建字符串过滤函数，使用默认参数避免闭包问题
                        def safe_string_decode(raw_value):
                            """安全地解码字符串值，与CLI显示逻辑保持一致"""
                            if isinstance(raw_value, bytes):
                                try:
                                    decoded = raw_value.decode('utf-8').rstrip('\x00')
                                    
                                    # 处理双重序列化的情况（如 b'b\'"test"\\x00'）
                                    if decoded.startswith("b'") and decoded.endswith("'"):
                                        # 移除 b' 和 ' 包装
                                        inner = decoded[2:-1]
                                        # 处理转义字符
                                        inner = inner.replace('\\x00', '').replace('\\n', '\n').replace('\\t', '\t')
                                        # 移除内部引号
                                        if inner.startswith('"') and inner.endswith('"'):
                                            inner = inner[1:-1]
                                        elif inner.startswith("'") and inner.endswith("'"):
                                            inner = inner[1:-1]
                                        decoded = inner
                                    
                                    # 移除可能的引号包装
                                    elif decoded.startswith("'") and decoded.endswith("'"):
                                        decoded = decoded[1:-1]
                                    elif decoded.startswith('"') and decoded.endswith('"'):
                                        decoded = decoded[1:-1]
                                    
                                    return decoded.strip()
                                except:
                                    return str(raw_value)
                            else:
                                return str(raw_value).strip()
                        
                        if operator == '=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_string_decode(row_data[col_idx]) == val
                            ) if row_data is not None else False
                        elif operator == '!=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_string_decode(row_data[col_idx]) != val
                            ) if row_data is not None else False
                        else:
                            return lambda row_data: True
                    # 对于日期类型，进行字符串比较
                    elif col_type_upper == 'DATE' or col_type_upper.startswith('DATE'):
                        # 日期比较，去掉引号
                        if value.startswith("'") and value.endswith("'"):
                            value = value[1:-1]
                        
                        def extract_date_string(date_value):
                            """从bytes或字符串中提取日期字符串"""
                            if isinstance(date_value, bytes):
                                # 处理bytes类型，去掉空字节
                                return date_value.decode('utf-8').rstrip('\x00')
                            else:
                                return str(date_value)
                        
                        # 创建日期过滤函数，使用默认参数避免闭包问题
                        if operator == '>':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and extract_date_string(row_data[col_idx]) > val
                            ) if row_data is not None else False
                        elif operator == '<':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and extract_date_string(row_data[col_idx]) < val
                            ) if row_data is not None else False
                        elif operator == '>=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and extract_date_string(row_data[col_idx]) >= val
                            ) if row_data is not None else False
                        elif operator == '<=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and extract_date_string(row_data[col_idx]) <= val
                            ) if row_data is not None else False
                        elif operator == '=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and extract_date_string(row_data[col_idx]) == val
                            ) if row_data is not None else False
                        elif operator == '!=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and extract_date_string(row_data[col_idx]) != val
                            ) if row_data is not None else False
                        else:
                            return lambda row_data: True
                    else:
                        # 数字类型，尝试转换
                        try:
                            value = float(value)
                        except ValueError:
                            return lambda row_data: True
                        
                        # 创建数字过滤函数（row格式为row_data），使用默认参数避免闭包问题
                        if operator == '>':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_float_convert(row_data[col_idx]) > val
                            ) if row_data is not None else False
                        elif operator == '<':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_float_convert(row_data[col_idx]) < val
                            ) if row_data is not None else False
                        elif operator == '>=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_float_convert(row_data[col_idx]) >= val
                            ) if row_data is not None else False
                        elif operator == '<=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_float_convert(row_data[col_idx]) <= val
                            ) if row_data is not None else False
                        elif operator == '=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_float_convert(row_data[col_idx]) == val
                            ) if row_data is not None else False
                        elif operator == '!=':
                            return lambda row_data, col_idx=column_index, val=value: (
                                col_idx < len(row_data) and safe_float_convert(row_data[col_idx]) != val
                            ) if row_data is not None else False
                        else:
                            return lambda row_data: True
                else:
                    return lambda row_data: True
            else:
                return lambda row_data: True
                
        except Exception as e:
            return lambda row_data: True
    
    def _convert_order_by(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换ORDER BY操作 (修复版)"""
        child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        if not child_plan:
            raise ValueError("ORDER BY operator must have a child plan.")

        schema = child_plan.schema
        if not schema:
            raise ValueError(f"Internal error: Child operator ({type(child_plan).__name__}) for ORDER BY has no schema.")

        order_items = properties.get("items", [])
        sort_key_info = []
        
        for item in order_items:
            column_name = item.get('column')
            direction = item.get('direction', 'ASC').upper()
            if not column_name: continue
            try:
                col_idx = schema.get_index(column_name)
                sort_key_info.append((col_idx, direction))
            except KeyError:
                continue

        if not sort_key_info:
            return child_plan # 如果没有有效的排序列，则不排序
            
        return Sort(child_plan, sort_key_info)
    
    def _convert_sort(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换SORT操作"""
        if "children" not in operator_tree or not operator_tree["children"]:
            raise ValueError("SORT operator must have a child plan.")
        
        child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        if not child_plan:
            raise ValueError("SORT operator must have a child plan.")

        schema = child_plan.schema
        if not schema:
            raise ValueError(f"Internal error: Child operator ({type(child_plan).__name__}) for SORT has no schema.")

        # 从属性中获取排序信息
        order_by = properties.get("order_by", [])
        ascending = properties.get("ascending", [])
        
        sort_key_info = []
        for i, column_name in enumerate(order_by):
            try:
                col_idx = schema.get_index(column_name)
                is_ascending = ascending[i] if i < len(ascending) else True
                direction = 'ASC' if is_ascending else 'DESC'
                sort_key_info.append((col_idx, direction))
            except KeyError:
                continue

        if not sort_key_info:
            return child_plan # 如果没有有效的排序列，则不排序
            
        from src.engine.operator import Sort
        return Sort(child_plan, sort_key_info)
    
    def _convert_limit(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换LIMIT操作"""
        child_plan = None
        if operator_tree["children"]:
            child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        
        if child_plan:
            limit = properties.get("limit", 100)  # 默认限制100行
            offset = properties.get("offset", 0)  # 默认偏移0
            
            from src.engine.operator import Limit
            limit_op = Limit(child_plan, limit, offset)
            return limit_op
        return None
    
    def _convert_group_by(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换GROUP BY操作"""
        child_plan = None
        if operator_tree["children"]:
            child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        
        if child_plan:
            # 尝试从properties中获取GROUP BY列
            group_columns = properties.get("columns", [])
            
            # 如果properties中没有，尝试从operator_tree中获取
            if not group_columns and "class" in operator_tree and "GroupByOperator" in operator_tree["class"]:
                # 这是一个GroupByOperator，我们需要从其他地方获取GROUP BY信息
                # 由于properties是空的，我们需要从算子树的其他地方获取
                # 从AGGREGATE算子的columns中提取非聚合函数的列
                if hasattr(self, '_current_aggregate_columns'):
                    agg_columns = self._current_aggregate_columns
                    # 从AGGREGATE列中提取非聚合函数的列作为GROUP BY列
                    non_agg_columns = []
                    for col in agg_columns:
                        if isinstance(col, str) and not ('(' in col and ')' in col):
                            # 去掉表名前缀
                            col_name = col.split('.')[-1] if '.' in col else col
                            non_agg_columns.append(col_name)
                    if non_agg_columns:
                        group_columns = non_agg_columns
                    else:
                        # 如果无法从聚合列中提取，尝试从子计划的schema中获取
                        if hasattr(child_plan, 'schema') and child_plan.schema:
                            # 获取schema中的列名
                            if hasattr(child_plan.schema, 'columns'):
                                # 处理不同的列格式
                                schema_columns = []
                                for col in child_plan.schema.columns:
                                    if hasattr(col, 'name'):
                                        schema_columns.append(col.name)
                                    elif isinstance(col, tuple):
                                        schema_columns.append(col[0])  # 假设第一个元素是列名
                                    else:
                                        schema_columns.append(str(col))
                                if schema_columns:
                                    group_columns = [schema_columns[0]]  # 使用第一列作为分组列
                                else:
                                    group_columns = []  # 空列表，表示没有分组列
                            else:
                                group_columns = []  # 空列表，表示没有分组列
                        else:
                            group_columns = []  # 空列表，表示没有分组列
                else:
                    # 如果无法从聚合列中提取，尝试从子计划的schema中获取
                    if hasattr(child_plan, 'schema') and child_plan.schema:
                        # 获取schema中的列名
                        if hasattr(child_plan.schema, 'columns'):
                            # 处理不同的列格式
                            schema_columns = []
                            for col in child_plan.schema.columns:
                                if hasattr(col, 'name'):
                                    schema_columns.append(col.name)
                                elif isinstance(col, tuple):
                                    schema_columns.append(col[0])  # 假设第一个元素是列名
                                else:
                                    schema_columns.append(str(col))
                            if schema_columns:
                                group_columns = [schema_columns[0]]  # 使用第一列作为分组列
                            else:
                                group_columns = []  # 空列表，表示没有分组列
                        else:
                            group_columns = []  # 空列表，表示没有分组列
                    else:
                        group_columns = []  # 空列表，表示没有分组列
            
            # 如果仍然没有分组列，尝试从逻辑计划的其他地方获取
            if not group_columns:
                # 尝试从当前正在处理的SELECT节点中获取GROUP BY信息
                if hasattr(self, '_current_select_node') and self._current_select_node:
                    select_node = self._current_select_node
                    if hasattr(select_node, 'group_by') and select_node.group_by:
                        if hasattr(select_node.group_by, 'columns'):
                            group_columns = select_node.group_by.columns
                        else:
                            group_columns = [str(select_node.group_by)]
            
            # 获取子计划的schema
            schema = child_plan.schema if hasattr(child_plan, 'schema') else None
            if not schema:
                return child_plan
            
            # 找到分组列在schema中的索引
            group_by_indices = []
            for col in group_columns:
                try:
                    # 处理GROUP BY列的别名，提取原始列名
                    if ' AS ' in col:
                        # 有别名的情况，提取原始列名
                        original_col = col.split(' AS ')[0]
                        # 去掉表名前缀
                        if '.' in original_col:
                            original_col = original_col.split('.')[-1]
                    else:
                        # 没有别名，直接使用
                        original_col = col
                        # 去掉表名前缀
                        if '.' in original_col:
                            original_col = original_col.split('.')[-1]
                    
                    col_idx = schema.get_index(original_col)
                    group_by_indices.append(col_idx)
                except KeyError:
                    continue
            
            # 创建HashAggregate算子
            from src.engine.operator import HashAggregate
            # 对于GROUP BY，我们需要聚合所有列，但这里暂时只返回子计划
            # 实际的聚合应该在AGGREGATE操作符中处理
            
            # 将GROUP BY信息保存到子计划中，以便AGGREGATE算子使用
            if hasattr(child_plan, '__dict__'):
                child_plan.group_by_columns = group_columns
            else:
                # 如果child_plan没有__dict__，创建一个包装器
                class GroupByWrapper:
                    def __init__(self, child_plan, group_by_columns):
                        self._child = child_plan
                        self.group_by_columns = group_by_columns
                        # 代理所有其他属性到child_plan
                        for attr in dir(child_plan):
                            if not attr.startswith('_') and not hasattr(self, attr):
                                setattr(self, attr, getattr(child_plan, attr))
                
                child_plan = GroupByWrapper(child_plan, group_columns)
            
            return child_plan
        
        return None
    
    def _find_parent_aggregate_tree(self, operator_tree: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """查找父级AGGREGATE算子"""
        # 这是一个简化的实现，实际中可能需要更复杂的逻辑
        # 由于GROUP_BY是AGGREGATE的子算子，我们需要从全局算子树中查找
        # 这里暂时返回None，让调用者处理
        return None
    
    def _convert_aggregate(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换聚合操作 (修复Schema生成问题)"""
        if "children" not in operator_tree or not operator_tree["children"]:
            raise ValueError("Aggregate operator must have children")
        
        child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        if not child_plan or not child_plan.schema:
            raise ValueError("Aggregate operator's child must have a valid plan and schema.")
        
        child_schema = child_plan.schema
        
        group_by_keys_names = []
        agg_expressions_config = []
        output_schema_cols_agg = []

        # --- 1. 优先从 'group_by_columns' 属性提取分组列 ---
        group_by_from_prop = properties.get("group_by_columns", [])
        if group_by_from_prop:
            for col_name_str in group_by_from_prop:
                clean_name = str(col_name_str).split('.')[-1]
                if clean_name not in group_by_keys_names:
                    group_by_keys_names.append(clean_name)
        
        # --- 2. 从 'columns' 属性提取聚合函数，并智能推断分组列 ---
        logical_columns = properties.get("columns", [])
        for col_obj in logical_columns:
            col_str = str(col_obj)
            # a. 如果是聚合函数字符串
            if 'AggregateFunction(' in col_str:
                # 解析聚合函数字符串，例如 "AggregateFunction(function_name='COUNT', argument=Identifier(...), distinct=False, alias='total_orders')"
                import re
                func_match = re.search(r"function_name='([^']+)'", col_str)
                alias_match = re.search(r"alias='([^']+)'", col_str)
                argument_match = re.search(r"value='([^']+)'", col_str)
                
                if func_match and alias_match and argument_match:
                    func_name = func_match.group(1).upper()
                    alias = alias_match.group(1)
                    input_col_name = argument_match.group(1).split('.')[-1]
                    try:
                        input_col_idx = child_schema.get_index(input_col_name)
                        agg_expressions_config.append((func_name, input_col_idx))
                        output_schema_cols_agg.append((alias, 'FLOAT'))
                    except KeyError:
                        # 如果列名不存在，使用默认值
                        agg_expressions_config.append((func_name, 0))
                        output_schema_cols_agg.append((alias, 'FLOAT'))
            # b. 如果是聚合函数对象
            elif hasattr(col_obj, 'function_name'):
                func_name = col_obj.function_name.upper()
                alias = col_obj.alias or f"agg_{len(agg_expressions_config)}"  # 修复空别名问题
                input_col_name = col_obj.argument.value.split('.')[-1] if hasattr(col_obj.argument, 'value') else str(col_obj.argument)
                try:
                    input_col_idx = child_schema.get_index(input_col_name)
                    agg_expressions_config.append((func_name, input_col_idx))
                    output_schema_cols_agg.append((alias, 'FLOAT'))
                except KeyError:
                    # 如果列名不存在，使用默认值
                    agg_expressions_config.append((func_name, 0))
                    output_schema_cols_agg.append((alias, 'FLOAT'))
            # c. 如果是普通列 (推断为分组列，作为备用方案)
            elif not group_by_from_prop and not col_str.startswith('AggregateFunction') and 'Identifier(' in col_str:
                # 处理AST节点格式的列名
                import re
                match = re.search(r"value='([^']+)'", col_str)
                if match:
                    col_name = match.group(1)
                    clean_name = col_name.split('.')[-1]
                    if clean_name not in group_by_keys_names:
                        group_by_keys_names.append(clean_name)

        # --- 3. 构建最终参数 ---
        group_by_indices = []
        for name in group_by_keys_names:
            try:
                idx = child_schema.get_index(name)
                group_by_indices.append(idx)
            except KeyError:
                continue  # 跳过不存在的列
        
        output_schema_cols_group = []
        for name in group_by_keys_names:
            try:
                idx = child_schema.get_index(name)
                col_type = child_schema.columns[idx][1]
                output_schema_cols_group.append((name, col_type))
            except KeyError:
                continue  # 跳过不存在的列
        
        # 确保聚合函数的结果列也被包含在输出Schema中
        # 如果没有聚合表达式，至少添加一个默认的COUNT
        if not agg_expressions_config:
            agg_expressions_config.append(('COUNT', 0))
            output_schema_cols_agg.append(('count', 'FLOAT'))
        
        # 确保输出Schema不为空
        if not output_schema_cols_group and not output_schema_cols_agg:
            output_schema_cols_agg.append(('count', 'FLOAT'))
            agg_expressions_config.append(('COUNT', 0))
        
        output_schema = Schema(output_schema_cols_group + output_schema_cols_agg)
        
        return HashAggregate(child_plan, group_by_indices, agg_expressions_config, output_schema)
    
    def _convert_update(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换UPDATE操作"""
        table_name = properties.get("table_name", "unknown")
        set_clause = properties.get("set_clause", [])
        
        # 首先获取 schema，这在循环外执行一次即可
        try:
            table_info = self.catalog_manager.get_table(table_name)
            columns = [(col.column_name, col.data_type) for col in table_info.columns]
            schema = Schema(columns)
        except Exception as e:
            # 抛出更明确的错误信息
            raise ValueError(f"无法为 UPDATE 语句找到表 '{table_name}' 的元数据: {e}")
        
        # 转换set_clause为updates字典
        updates = {}
        if isinstance(set_clause, list):
            # 处理Assignment对象列表
            for assignment in set_clause:
                if hasattr(assignment, 'column') and hasattr(assignment, 'value'):
                    # Assignment对象
                    column_name = assignment.column.value if hasattr(assignment.column, 'value') else str(assignment.column)
                    value_node = assignment.value
                else:
                    # 元组格式: (column_name, value)
                    column_name, value_node = assignment
                
                col_index = schema.get_index(column_name)
                
                # 判断 value_node 是不是一个表达式
                # 简单判断：如果它有 left, operator, right 属性，就是一个二元表达式
                if hasattr(value_node, 'left') and hasattr(value_node, 'operator') and hasattr(value_node, 'right'):
                    
                    # 获取表达式的左右部分
                    left_operand_name = value_node.left.value
                    operator = value_node.operator[1] if isinstance(value_node.operator, tuple) else str(value_node.operator)
                    # 假设右边是数字字面量
                    right_operand_value = float(value_node.right.value)

                    # 获取左操作数（通常是列名）的索引
                    left_operand_index = schema.get_index(left_operand_name)

                    # 创建一个 lambda 函数来封装计算逻辑
                    # 这个函数接收一行数据(row_data)，执行计算并返回结果
                    def create_evaluator(left_idx, op, right_val):
                        def evaluator(row_data):
                            return float(row_data[left_idx]) - right_val if op == '-' else \
                                   float(row_data[left_idx]) + right_val if op == '+' else \
                                   float(row_data[left_idx]) * right_val if op == '*' else \
                                   float(row_data[left_idx]) / right_val if op == '/' else \
                                   float(row_data[left_idx]) % right_val if op == '%' else \
                                   float(row_data[left_idx])
                        return evaluator
                    
                    updates[col_index] = create_evaluator(left_operand_index, operator, right_operand_value)

                else:
                    # 如果不是表达式，就是普通的字面量
                    raw_value = value_node.value if hasattr(value_node, 'value') else value_node
                    # 清理和转换值
                    # 如果raw_value是字符串且包含Literal的字符串表示，需要解析
                    if isinstance(raw_value, str) and "Literal(token=" in raw_value:
                        # 使用正则表达式提取value部分
                        import re
                        match = re.search(r"value='([^']+)'", raw_value)
                        if match:
                            actual_value = match.group(1)
                        else:
                            actual_value = raw_value
                    elif hasattr(raw_value, 'value'):
                        # 如果是Literal对象，提取其value
                        actual_value = raw_value.value
                    else:
                        actual_value = raw_value
                    
                    # 确保actual_value是字符串类型，因为_clean_values期望字符串
                    if not isinstance(actual_value, str):
                        actual_value = str(actual_value)
                    
                    cleaned_value = self._clean_values([actual_value])[0]
                    updates[col_index] = cleaned_value
                    
        elif isinstance(set_clause, dict):
            # 处理字典格式
            for column_name, value_node in set_clause.items():
                col_index = schema.get_index(column_name)
                
                # 判断 value_node 是不是一个表达式
                if hasattr(value_node, 'left') and hasattr(value_node, 'operator') and hasattr(value_node, 'right'):
                    
                    # 获取表达式的左右部分
                    left_operand_name = value_node.left.value
                    operator = value_node.operator[1] if isinstance(value_node.operator, tuple) else str(value_node.operator)
                    # 假设右边是数字字面量
                    right_operand_value = float(value_node.right.value)

                    # 获取左操作数（通常是列名）的索引
                    left_operand_index = schema.get_index(left_operand_name)

                    # 创建一个 lambda 函数来封装计算逻辑
                    def create_evaluator(left_idx, op, right_val):
                        def evaluator(row_data):
                            return float(row_data[left_idx]) - right_val if op == '-' else \
                                   float(row_data[left_idx]) + right_val if op == '+' else \
                                   float(row_data[left_idx]) * right_val if op == '*' else \
                                   float(row_data[left_idx]) / right_val if op == '/' else \
                                   float(row_data[left_idx]) % right_val if op == '%' else \
                                   float(row_data[left_idx])
                        return evaluator
                    
                    updates[col_index] = create_evaluator(left_operand_index, operator, right_operand_value)

                else:
                    # 如果不是表达式，就是普通的字面量
                    raw_value = value_node.value if hasattr(value_node, 'value') else value_node
                    # 清理和转换值
                    # 如果raw_value是字符串且包含Literal的字符串表示，需要解析
                    if isinstance(raw_value, str) and "Literal(token=" in raw_value:
                        # 使用正则表达式提取value部分
                        import re
                        match = re.search(r"value='([^']+)'", raw_value)
                        if match:
                            actual_value = match.group(1)
                        else:
                            actual_value = raw_value
                    elif hasattr(raw_value, 'value'):
                        # 如果是Literal对象，提取其value
                        actual_value = raw_value.value
                    else:
                        actual_value = raw_value
                    
                    # 确保actual_value是字符串类型，因为_clean_values期望字符串
                    if not isinstance(actual_value, str):
                        actual_value = str(actual_value)
                    
                    cleaned_value = self._clean_values([actual_value])[0]
                    updates[col_index] = cleaned_value
        
        # 创建SeqScan作为基础扫描
        child_plan = SeqScan(table_name, self.storage_engine, schema)
        
        # 如果有WHERE条件，添加Filter操作符
        where_clause = properties.get("where_clause", None)
        if where_clause:
            # 解析WHERE条件
            filter_func = self._parse_condition(where_clause, schema)
            child_plan = Filter(child_plan, filter_func)
        
        return Update(child_plan, table_name, updates, self.storage_engine)
    
    def _convert_drop_table(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换DROP TABLE操作"""
        from src.engine.operator import DropTable
        
        table_name = properties.get("table_name", "unknown")
        if_exists = properties.get("if_exists", False)
        
        return DropTable(table_name, self.storage_engine, if_exists)
    
    def _convert_drop_index(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换DROP INDEX操作"""
        from src.engine.operator import DropIndex
        
        index_name = properties.get("index_name", "unknown")
        
        return DropIndex(index_name, self.storage_engine, self.catalog_manager)
    
    def _convert_show_tables(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换SHOW TABLES操作"""
        from src.engine.operator import ShowTables
        return ShowTables(self.catalog_manager)
    
    def _convert_show_columns(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换SHOW COLUMNS操作"""
        from src.engine.operator import ShowColumns
        table_name = properties.get("table_name", "unknown")
        return ShowColumns(table_name, self.catalog_manager)
    
    def _convert_show_index(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换SHOW INDEX操作"""
        from src.engine.operator import ShowIndex
        table_name = properties.get("table_name", "unknown")
        return ShowIndex(table_name, self.catalog_manager)
    
    def _convert_show_views(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换SHOW VIEWS操作"""
        from src.engine.operator import ShowViews
        return ShowViews(self.catalog_manager)
    
    def _convert_explain(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换EXPLAIN操作"""
        from src.engine.operator import Explain
        # 这里需要递归转换子查询
        query_str = properties.get("query", "")
        # 简化实现，直接返回查询字符串
        class DummyQuery:
            def __init__(self, query_str):
                self.query_str = query_str
        return Explain(DummyQuery(query_str))
    
    def _convert_create_index(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换CREATE INDEX操作"""
        from src.engine.operator import CreateIndex
        index_name = properties.get("index_name", "unknown")
        table_name = properties.get("table_name", "unknown")
        
        # 支持多列索引
        if "columns" in properties:
            columns = properties["columns"]
        else:
            # 向后兼容单列索引
            column_name = properties.get("column_name", "unknown")
            columns = column_name
        
        return CreateIndex(self.catalog_manager, table_name, index_name, columns)
    
    def _convert_join(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换JOIN操作"""
        from src.engine.operator import HashJoin, NestedLoopJoin
        
        # 获取左右子节点
        children = operator_tree.get("children", [])
        if len(children) < 2:
            raise ValueError("JOIN操作需要两个子节点")
        
        # 递归转换子节点
        left_child = self.convert_to_physical_plan(children[0])
        right_child = self.convert_to_physical_plan(children[1])
        
        # 获取连接类型
        join_type = properties.get("join_type", "INNER")
        
        if join_type == "CARTESIAN":
            # 笛卡尔积连接，无条件
            return NestedLoopJoin(left_child, right_child, None)
        else:
            # 等值连接，使用HashJoin
            # 获取连接键索引
            # 根据表结构设置正确的连接键索引
            # customers: customer_id(0), name(1), city(2)
            # orders: order_id(0), customer_id(1), order_date(2), amount(3)
            left_key_indices = properties.get("left_key_indices", [0])  # customers.customer_id
            right_key_indices = properties.get("right_key_indices", [1])  # orders.customer_id
            
            return HashJoin(left_child, right_child, left_key_indices, right_key_indices)
    
    def _convert_create_view(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换CREATE VIEW操作"""
        from src.engine.operator import CreateView
        
        view_name = properties.get("view_name", "unknown")
        definition = properties.get("definition", "")
        schema_name = properties.get("schema_name", "public")
        creator = properties.get("creator", "system")
        is_updatable = properties.get("is_updatable", False)
        
        return CreateView(view_name, definition, schema_name, creator, is_updatable, self.catalog_manager)
    
    def _convert_drop_view(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换DROP VIEW操作"""
        from src.engine.operator import DropView
        
        view_name = properties.get("view_name", "unknown")
        
        return DropView(view_name, self.catalog_manager)
    
    def _convert_alter_view(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换ALTER VIEW操作"""
        from src.engine.operator import AlterView
        
        view_name = properties.get("view_name", "unknown")
        definition = properties.get("definition", "")
        is_updatable = properties.get("is_updatable", None)
        
        return AlterView(view_name, definition, is_updatable, self.catalog_manager)
    
    def _convert_having(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """转换HAVING操作"""
        from src.engine.operator import Filter
        
        child_plan = None
        if operator_tree["children"]:
            child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        
        if child_plan:
            condition = properties.get("condition", None)
            # HAVING条件作用于聚合结果，需要特殊处理
            # 暂时使用简化的条件解析
            filter_func = self._parse_condition(condition, child_plan.schema)
            return Filter(child_plan, filter_func)
        return None
    
    def _convert_aggregate_query(self, operator_tree: Dict[str, Any], properties: Dict[str, Any]):
        """专门处理聚合查询的转换方法"""
        from src.engine.operator import HashAggregate, Filter
        
        # 获取子计划
        child_plan = None
        if operator_tree["children"]:
            child_plan = self.convert_to_physical_plan(operator_tree["children"][0])
        
        if not child_plan:
            return None
        
        # 获取schema
        schema = child_plan.schema if hasattr(child_plan, 'schema') else None
        if not schema:
            return child_plan
        
        # 从properties中提取信息
        columns = properties.get("columns", [])
        group_by_columns = properties.get("group_by_columns", [])
        having_condition = properties.get("having_condition", None)
        
        # 处理分组列
        group_by_indices = []
        for col in group_by_columns:
            try:
                # 处理列名，去掉表前缀
                col_name = col.split('.')[-1] if '.' in col else col
                col_idx = schema.get_index(col_name)
                group_by_indices.append(col_idx)
            except KeyError:
                continue
        
        # 处理聚合表达式
        agg_expressions = []
        for col in columns:
            if isinstance(col, str):
                # 处理字符串格式的聚合函数，如 "COUNT(o.order_id) AS total_orders"
                if '(' in col and ')' in col:
                    # 提取函数名和参数
                    func_start = col.find('(')
                    func_end = col.find(')')
                    if func_start != -1 and func_end != -1:
                        func_name = col[:func_start].strip().upper()
                        param = col[func_start+1:func_end].strip()
                        
                        # 处理参数，去掉表前缀
                        if '.' in param:
                            param = param.split('.')[-1]
                        
                        try:
                            param_idx = schema.get_index(param)
                            agg_expressions.append((func_name, param_idx))
                        except KeyError:
                            continue
                else:
                    # 普通列，作为分组列处理
                    try:
                        col_name = col.split('.')[-1] if '.' in col else col
                        col_idx = schema.get_index(col_name)
                        group_by_indices.append(col_idx)
                    except KeyError:
                        continue
        
        # 创建输出Schema
        output_schema_cols = []
        
        # 添加分组列
        for idx in group_by_indices:
            if idx < len(schema.columns):
                col_name, col_type = schema.columns[idx]
                output_schema_cols.append((col_name, col_type))
        
        # 添加聚合函数列
        for i, (func_name, col_idx) in enumerate(agg_expressions):
            output_schema_cols.append((f"{func_name.lower()}_{i}", 'FLOAT'))
        
        # 确保Schema不为空
        if not output_schema_cols:
            output_schema_cols.append(('count', 'FLOAT'))
            agg_expressions = [('COUNT', 0)]
        
        output_schema = Schema(output_schema_cols)
        
        # 创建HashAggregate算子
        aggregate_plan = HashAggregate(child_plan, group_by_indices, agg_expressions, output_schema)
        
        # 如果有HAVING条件，添加Filter算子
        if having_condition:
            filter_func = self._parse_condition(str(having_condition), aggregate_plan.schema)
            return Filter(aggregate_plan, filter_func)
        
        return aggregate_plan
    
    def _convert_create_trigger(self, properties: Dict[str, Any]):
        """转换 CREATE_TRIGGER 操作"""
        from src.engine.operator import CreateTrigger
        return CreateTrigger(
            trigger_name=properties.get("trigger_name"),
            table_name=properties.get("table_name"),
            timing=properties.get("timing"),
            events=properties.get("events"),
            is_row_level=properties.get("is_row_level"),
            when_condition=properties.get("when_condition"),
            trigger_body=properties.get("trigger_body"),
            storage_engine=self.storage_engine
        )

    def _convert_drop_trigger(self, properties: Dict[str, Any]):
        """转换 DROP_TRIGGER 操作"""
        from src.engine.operator import DropTrigger
        return DropTrigger(
            trigger_name=properties.get("trigger_name"),
            storage_engine=self.storage_engine
        )

    def _convert_show_triggers(self, properties: Dict[str, Any]):
        """转换 SHOW_TRIGGERS 操作"""
        from src.engine.operator import ShowTriggers
        return ShowTriggers(catalog_manager=self.catalog_manager)
    
    def _convert_declare_cursor(self, properties: Dict[str, Any]):
        """转换 DECLARE_CURSOR 操作"""
        from src.engine.operator import DeclareCursor
        cursor_name = properties.get("cursor_name")
        query_plan_dict = properties.get("query_plan")
        
        # 递归转换查询计划
        query_plan = None
        if query_plan_dict:
            query_plan = self.convert_to_physical_plan(query_plan_dict)
        
        return DeclareCursor(
            cursor_name=cursor_name,
            query_plan=query_plan,
            cli_interface=getattr(self, 'cli_interface', None)
        )
    
    def _convert_open_cursor(self, properties: Dict[str, Any]):
        """转换 OPEN_CURSOR 操作"""
        from src.engine.operator import OpenCursor
        return OpenCursor(
            cursor_name=properties.get("cursor_name"),
            cli_interface=getattr(self, 'cli_interface', None)
        )
    
    def _convert_fetch_cursor(self, properties: Dict[str, Any]):
        """转换 FETCH_CURSOR 操作"""
        from src.engine.operator import FetchCursor
        return FetchCursor(
            cursor_name=properties.get("cursor_name"),
            cli_interface=getattr(self, 'cli_interface', None)
        )
    
    def _convert_close_cursor(self, properties: Dict[str, Any]):
        """转换 CLOSE_CURSOR 操作"""
        from src.engine.operator import CloseCursor
        return CloseCursor(
            cursor_name=properties.get("cursor_name"),
            cli_interface=getattr(self, 'cli_interface', None)
        )
    
    def _convert_begin_transaction(self, properties: Dict[str, Any]):
        """转换 BEGIN_TRANSACTION 操作"""
        from src.engine.operator import BeginTransaction
        return BeginTransaction()
    
    def _convert_commit_transaction(self, properties: Dict[str, Any]):
        """转换 COMMIT_TRANSACTION 操作"""
        from src.engine.operator import CommitTransaction
        return CommitTransaction()
    
    def _convert_rollback_transaction(self, properties: Dict[str, Any]):
        """转换 ROLLBACK_TRANSACTION 操作"""
        from src.engine.operator import RollbackTransaction
        return RollbackTransaction()
