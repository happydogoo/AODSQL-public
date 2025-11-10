# -*- coding: utf-8 -*-
"""
CLI接口模块
封装命令行交互逻辑和用户界面
"""

from typing import Dict, Any
from cli.system_manager import SystemManager
from cli.plan_converter import PlanConverter
from enum import Enum
from dataclasses import dataclass
import time
from src.engine.executor import Executor
from loguru import logger
from rich.console import Console
from rich.table import Table
import logging

console = Console()
logger.remove()
logger.add(lambda msg: print(msg, end=''), level="WARNING")

class CursorStatus(Enum):
    """游标状态枚举"""
    DECLARED = "DECLARED"
    OPEN = "OPEN"
    CLOSED = "CLOSED"


@dataclass
class CursorInfo:
    """游标信息数据类"""
    name: str
    plan: Any  # 存储已编译好的、可执行的物理计划
    status: CursorStatus = CursorStatus.DECLARED


class CursorStatus(Enum):
    """游标状态枚举"""
    DECLARED = "DECLARED"
    OPEN = "OPEN"
    CLOSED = "CLOSED"


@dataclass
class CursorInfo:
    """游标信息数据类"""
    name: str
    plan: Any  # 存储已编译好的、可执行的物理计划
    status: CursorStatus = CursorStatus.DECLARED


class CLIInterface:
    """命令行接口类"""
    
    def __init__(self, system_manager: SystemManager):
        # self.storage_engine = storage_engine
        # self.catalog_manager = catalog_manager
        # self.symbol_table = symbol_table
        # self.sql_interpreter = sql_interpreter
        # self.executor = executor
        # self.plan_converter = None
        self.system_manager = system_manager
        self.plan_converter = None
        self.cursors: Dict[str, CursorInfo] = {}  # 用于存储当前会话的所有游标
        self._current_transaction = None  # 当前手动事务
        self._is_manual_transaction = False  # 是否为手动事务标志
    
    def print_welcome(self):
        """打印欢迎信息"""
        print("欢迎来到 AODSQL 数据库系统！")
        print("输入 'quit' 退出，输入 'help' 查看帮助。")
        print("=" * 50)
    
    def print_help(self):
        """打印帮助信息"""
        logger.info("AODSQL 数据库系统帮助：")
        print("=" * 50)
        print("支持的SQL语句：")
        print("  CREATE DATABASE database_name;")
        print("  DROP DATABASE database_name;")
        print("  USE database_name;")
        print("  SHOW DATABASES;")
        print("  CREATE TABLE table_name (col1 type1, col2 type2, ...);")
        print("  INSERT INTO table_name VALUES (val1, val2, ...);")
        print("  SELECT col1, col2 FROM table_name WHERE condition;")
        print("  DELETE FROM table_name WHERE condition;")
        print("  UPDATE table_name SET col1=val1 WHERE condition;")
        print("\n多行SQL语句：")
        print("  支持多行SQL语句，以分号(;)结尾")
        print("  空行将被忽略")
        print("\n示例：")
        print("  单行：")
        print("    CREATE TABLE users (id INT, name VARCHAR, age INT);")
        print("    INSERT INTO users VALUES (1, 'Alice', 25);")
        print("    SELECT name, age FROM users WHERE age > 20;")
        print("    SELECT * FROM users;")
        print("\n  多行：")
        print("    CREATE TABLE departments (")
        print("        id INT PRIMARY KEY,")
        print("        name VARCHAR(50) NOT NULL,")
        print("        location VARCHAR(100)")
        print("    );")
        print("\n命令：")
        print("  help - 显示此帮助信息")
        print("  quit/exit/q - 退出系统")
    
    def print_operator_tree(self, tree: Dict[str, Any], indent: int = 0):
        """打印算子树结构"""
        prefix = "  " * indent
        op_type = tree["type"]
        properties = tree.get("properties", {})
        
        # 打印操作符
        print(f"{prefix}├─ {op_type}")
        
        # 打印重要属性
        important_props = ["table_name", "columns", "condition", "values", "predicate"]
        for prop in important_props:
            if prop in properties and properties[prop]:
                value = properties[prop]
                if isinstance(value, list) and len(value) > 3:
                    print(f"{prefix}│  {prop}: [{len(value)} items]")
                elif prop == "predicate":
                    # 对于predicate，只显示类型信息，不尝试字符串化
                    if callable(value):
                        print(f"{prefix}│  {prop}: <过滤函数>")
                    else:
                        print(f"{prefix}│  {prop}: {value}")
                else:
                    print(f"{prefix}│  {prop}: {value}")
        
        # 递归打印子节点
        children = tree.get("children", [])
        for child in children:
            self.print_operator_tree(child, indent + 1)

    def print_physical_plan(self, node: Any, indent: int = 0):
        """新的物理计划打印入口"""
        print("\nPhysical Execution Plan:")
        self._print_plan_node(node, "", True)

    def _print_plan_node(self, node: Any, prefix: str, is_last: bool):
        """递归打印计划节点"""
        # 构造连接符
        connector = "└─" if is_last else "├─"
        line = f"{prefix}{connector} "

        # 1. 获取基本信息
        node_type = type(node).__name__
        cost = getattr(node, 'metadata', {}).get('estimated_cost')
        rows = getattr(node, 'metadata', {}).get('estimated_rows')
        
        # 2. 格式化核心信息
        info = f"{node_type}"
        if cost is not None and rows is not None:
            info += f"  (cost={cost:.2f} rows={int(rows)})"
        
        line += info

        # 3. 补充各算子特有的关键信息
        details = []
        if hasattr(node, 'table_name'):
            details.append(f"on: {node.table_name}")
        if hasattr(node, 'condition'): # Filter算子
            details.append(f"cond: {node.condition}")
        if hasattr(node, 'sort_key_info'): # Sort算子
            sort_keys = []
            for idx, direction in node.sort_key_info:
                # 尝试从子节点的schema获取列名
                col_name = f"col_{idx}"
                if hasattr(node, 'child') and node.child.schema and len(node.child.schema) > idx:
                    col_name = node.child.schema[idx][0]
                sort_keys.append(f"{col_name} {direction}")
            details.append(f"by: [{', '.join(sort_keys)}]")
        
        if details:
            line += f"  [{' | '.join(details)}]"
        
        print(line)

        # 4. 准备递归打印子节点
        children = []
        if hasattr(node, 'child') and node.child:
            children.append(node.child)
        # 为JOIN等双子节点算子做准备
        if hasattr(node, 'left_child') and node.left_child:
            children.append(node.left_child)
        if hasattr(node, 'right_child') and node.right_child:
            children.append(node.right_child)

        # 5. 递归调用
        new_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(children):
            self._print_plan_node(child, new_prefix, i == len(children) - 1)

    def print_physical_plan_with_analysis(self, node: Any, analyze: bool = False):
        """带分析的物理计划打印"""
        if analyze:
            print("\nEXPLAIN ANALYZE Result:")
            print("=" * 60)
        else:
            print("\nPhysical Execution Plan:")
            print("=" * 40)
        
        self._print_plan_node_with_analysis(node, "", True, analyze)
        
        if analyze:
            self._print_performance_summary(node)

    def _print_plan_node_with_analysis(self, node: Any, prefix: str, is_last: bool, analyze: bool = False):
        """带分析的递归打印计划节点"""
        # 构造连接符
        connector = "└─" if is_last else "├─"
        line = f"{prefix}{connector} "

        # 1. 获取基本信息
        node_type = type(node).__name__
        cost = getattr(node, 'metadata', {}).get('estimated_cost')
        rows = getattr(node, 'metadata', {}).get('estimated_rows')
        
        # 2. 格式化核心信息
        info = f"{node_type}"
        if cost is not None and rows is not None:
            info += f"  (cost={cost:.2f} rows={int(rows)})"
        
        # 3. 如果是ANALYZE模式，添加实际执行信息
        if analyze and hasattr(node, '_profile_data'):
            actual_time = node._profile_data.get('time_ms', 0)
            actual_rows = node._profile_data.get('rows', 0)
            actual_calls = node._profile_data.get('calls', 0)
            info += f" (actual_time={actual_time:.2f}ms rows={actual_rows} loops={actual_calls})"
        
        line += info

        # 4. 补充各算子特有的关键信息
        details = []
        if hasattr(node, 'table_name'):
            details.append(f"on: {node.table_name}")
        if hasattr(node, 'condition'): # Filter算子
            details.append(f"cond: {node.condition}")
        if hasattr(node, 'sort_key_info'): # Sort算子
            sort_keys = []
            for idx, direction in node.sort_key_info:
                col_name = f"col_{idx}"
                if hasattr(node, 'child') and node.child.schema and len(node.child.schema) > idx:
                    col_name = node.child.schema[idx][0]
                sort_keys.append(f"{col_name} {direction}")
            details.append(f"by: [{', '.join(sort_keys)}]")
        
        if details:
            line += f"  [{' | '.join(details)}]"
        
        print(line)

        # 5. 准备递归打印子节点
        children = []
        if hasattr(node, 'child') and node.child:
            children.append(node.child)
        if hasattr(node, 'left_child') and node.left_child:
            children.append(node.left_child)
        if hasattr(node, 'right_child') and node.right_child:
            children.append(node.right_child)

        # 6. 递归调用
        new_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(children):
            self._print_plan_node_with_analysis(child, new_prefix, i == len(children) - 1, analyze)

    def _print_performance_summary(self, node: Any):
        """打印性能摘要"""
        total_time = 0
        total_rows = 0
        total_calls = 0
        
        def collect_stats(n):
            nonlocal total_time, total_rows, total_calls
            if hasattr(n, '_profile_data'):
                total_time += n._profile_data.get('time_ms', 0)
                total_rows += n._profile_data.get('rows', 0)
                total_calls += n._profile_data.get('calls', 0)
            
            # 递归收集子节点统计
            if hasattr(n, 'child') and n.child:
                collect_stats(n.child)
            if hasattr(n, 'left_child') and n.left_child:
                collect_stats(n.left_child)
            if hasattr(n, 'right_child') and n.right_child:
                collect_stats(n.right_child)
        
        collect_stats(node)
        
        print(f"\nExecution Statistics:")
        print(f"    Total execution time: {total_time:.2f}ms")
        print(f"    Total processed rows: {total_rows:,}")
        print(f"    Total calls: {total_calls}")
        if total_calls > 0:
            print(f"    Average call time: {total_time/total_calls:.2f}ms")
            print(f"    Average calls per row: {total_rows/total_calls:.1f}")

    def handle_explain_analyze(self, sql_input: str):
        """处理EXPLAIN ANALYZE命令"""
        try:
            # 提取实际查询语句
            query_sql = sql_input[15:].strip()  # 移除"EXPLAIN ANALYZE"
            if not query_sql:
                print("❌ Please provide the SQL query to analyze.")
                return
            
            print(f"�� Analyzing query: {query_sql}")
            
            # 1. 编译SQL
            result = self.sql_interpreter.interpret(query_sql)
            if result["status"] == "error":
                print(f"❌ Compilation failed: {result['message']}")
                return
            
            # 2. 转换为物理计划
            operator_tree = result["operator_tree"]
            physical_plan = self.plan_converter.convert_to_physical_plan(operator_tree)
            
            if not physical_plan:
                print("❌ Could not generate physical execution plan.")
                return
            
            # 3. 执行查询以收集性能数据
            print("⚡ Executing query to collect performance data...")
            start_time = time.time()
            
            # 执行查询
            if hasattr(physical_plan, 'next'):
                # 查询型算子
                while True:
                    batch = physical_plan.next()
                    if batch is None:
                        break
            else:
                # 终止型算子
                physical_plan.execute()
            
            execution_time = time.time() - start_time
            print(f"✅ Query execution completed, time: {execution_time:.3f} seconds")
            
            # 4. 显示带分析的执行计划
            self.print_physical_plan_with_analysis(physical_plan, analyze=True)
            
        except Exception as e:
            print(f"❌ EXPLAIN ANALYZE execution failed: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def process_sql_input(self, sql_input: str) -> bool:
        """
        处理SQL输入
        返回True表示继续运行，False表示退出
        """
        try:
            sql_stripped=sql_input.strip().lower()

# --- 步骤 A: 优先处理不需要数据库上下文的全局命令 ---
            if sql_stripped.startswith('create database'):
                db_name = sql_input.split()[2].strip(';')
                self.system_manager.create_database(db_name)
                print(f"✅ Database '{db_name}' created successfully.")
                return True

            if sql_stripped.startswith('drop database'):
                db_name = sql_input.split()[2].strip(';')
                self.system_manager.drop_database(db_name)
                print(f"🗑️ Database '{db_name}' deleted.")
                return True
            
            if sql_stripped.startswith('use '):
                db_name = sql_input.split()[1].strip(';')
                self.system_manager.use_database(db_name)
                # 成功切换数据库后，不需要做额外操作，get_current_components 会处理
                return True
            
# --- 步骤 B: 对于其他SQL，必须先获取当前数据库的组件 ---
            # 这是新的核心逻辑！
            components = self.system_manager.get_current_components()
            sql_interpreter = components['sql_interpreter']
            self.executor: Executor = components['executor']
            storage_engine = components['storage_engine']
            self.catalog_manager = components['catalog_manager']
            self.transaction_manager = components['transaction_manager']
            self.symbol_table = components['symbol_table']
            
            current_db = self.system_manager.current_db_name


            # 编译SQL
            result = sql_interpreter.interpret(sql_input)
            
            if result["status"] == "error":
                print(f"❌ Compilation failed: {result['message']}")
                return True
            
            # 显示算子树
            print("\nGenerated Operator Tree:")
            self.print_operator_tree(result["operator_tree"])
            

            # 显示优化信息
            metadata = result.get("metadata", {})
            if metadata:
                optimization_info = metadata.get("optimization", {})
                if optimization_info:
                    self._print_optimization_info(optimization_info)
            
            # 转换为物理计划并执行
            print("\nExecuting Operator Tree:")
            operator_tree = result["operator_tree"]
            plan_converter = PlanConverter(storage_engine, self.catalog_manager)
            physical_plan = plan_converter.convert_to_physical_plan(operator_tree)

            if physical_plan:
                # 打印物理计划结构
                self.print_physical_plan(physical_plan)
                
                # 检查是否是事务命令
                if self._is_transaction_command(physical_plan):
                    execution_result = self._handle_transaction_command(physical_plan)
                # 检查是否是游标命令
                elif self._is_cursor_command(physical_plan):
                    execution_result = self._handle_cursor_command(physical_plan)
                else:
                    # 检查是否有活跃的手动事务
                    if self._is_manual_transaction and self._current_transaction:
                        try:
                            execution_result = self.executor.execute_plan(physical_plan, self._current_transaction)
                            print(f"Query OK, 1 row affected (in transaction {self._current_transaction.id})")
                        except Exception as e:
                            print(f"❌ Operation failed: {str(e)}")
                            raise e
                    else:
                        # 创建自动事务并执行计划
                        if self.transaction_manager:
                            transaction = self.transaction_manager.begin()
                            try:
                                execution_result = self.executor.execute_plan(physical_plan, transaction)
                                self.transaction_manager.commit(transaction)
                                # 针对DDL操作输出MySQL风格提示
                                if operator_tree["type"] == "CREATE_TABLE":
                                    print(f"Query OK, table '{operator_tree['properties']['table_name']}' created.")
                                elif operator_tree["type"] == "DROP_TABLE":
                                    print(f"Query OK, table '{operator_tree['properties']['table_name']}' dropped.")
                                elif operator_tree["type"] == "CREATE_INDEX":
                                    print(f"Query OK, index '{operator_tree['properties']['index_name']}' created.")
                                elif operator_tree["type"] == "DROP_INDEX":
                                    print(f"Query OK, index '{operator_tree['properties']['index_name']}' dropped.")
                                else:
                                    print(f"Query OK, 1 row affected (automatic transaction committed)")
                            except Exception as e:
                                self.transaction_manager.abort(transaction)
                                raise e
                        else:
                            from src.engine.transaction.transaction import Transaction, IsolationLevel
                            transaction = Transaction(1, IsolationLevel.READ_COMMITTED)
                            execution_result = self.executor.execute_plan(physical_plan, transaction)
                            print(f"Query OK, 1 row affected (automatic transaction committed)")

                
                # 根据操作类型显示不同的结果格式
                if operator_tree["type"] in ["SELECT", "PROJECT", "ORDER_BY", "AGGREGATE", "LIMIT", "SORT"]:
                    self._display_query_result(execution_result, operator_tree)
                elif operator_tree["type"] == "SHOW_TABLES":
                    # 对于SHOW TABLES命令，特殊处理显示格式
                    self._display_show_tables_result(execution_result)
                elif operator_tree["type"] in ["BEGIN_TRANSACTION", "COMMIT_TRANSACTION", "ROLLBACK_TRANSACTION"]:
                    # 对于事务命令，显示执行结果
                    print()
                else:
                    # 对于其他命令，显示执行结果
                    print()
                
                # 如果是CREATE TABLE操作，同步到symbol_table
                if operator_tree["type"] == "CREATE_TABLE":
                    self._sync_table_to_symbol_table(physical_plan)
                
                # 如果是DROP TABLE操作，从symbol_table中移除表
                if operator_tree["type"] == "DROP_TABLE":
                    self._remove_table_from_symbol_table(operator_tree["properties"]["table_name"])
                
                # 对于修改数据的操作，立即刷新到磁盘
                if operator_tree["type"] in ["INSERT", "UPDATE", "DELETE", "CREATE_TABLE", "DROP_TABLE"]:
                    # self._flush_data_to_disk()
                    storage_engine.flush_all_tables()
                    # 保存catalog信息
                    self.catalog_manager._save_catalog()
            else:
                print("❌ Could not convert to physical execution plan.")
            
            return True
            
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            return False
        except Exception as e:
            print(f"❌ Execution error: {str(e)}")
            import traceback
            traceback.print_exc()
            return True
    
    def _flush_data_to_disk(self):
        """刷新数据到磁盘"""
        try:
            if hasattr(self.storage_engine, 'flush_all_tables'):
                self.storage_engine.flush_all_tables()
        except Exception as e:
            print(f"⚠️  Error flushing data to disk: {e}")
    
    def _is_aggregate_query(self, operator_tree):
        """检查是否是聚合查询"""
        if operator_tree.get("type") == "AGGREGATE":
            return True
        elif "children" in operator_tree and operator_tree["children"]:
            for child in operator_tree["children"]:
                if self._is_aggregate_query(child):
                    return True
        return False
    
    def _display_aggregate_result(self, execution_result, operator_tree):
        """显示聚合函数结果（使用Rich）"""
        properties = operator_tree.get("properties", {})
        columns = properties.get("columns", [])
        if not execution_result:
            console.print("[bold yellow]查询结果: 无数据[/bold yellow]")
            return
        table = Table(show_header=True, header_style="bold cyan")
        col_names = []
        for col in columns:
            if isinstance(col, str):
                if 'AggregateFunction' in col:
                    # 处理聚合函数字符串
                    import re
                    alias_match = re.search(r"alias='([^']+)'", col)
                    if alias_match:
                        col_names.append(alias_match.group(1))
                    else:
                        # 提取函数名
                        func_match = re.search(r"function_name='([^']+)'", col)
                        if func_match:
                            col_names.append(func_match.group(1).lower())
                        else:
                            col_names.append("aggregate")
                elif 'Identifier(' in col or '.Identifier(' in col:
                    # 处理AST节点格式的列名
                    import re
                    match = re.search(r"value='([^']+)'", col)
                    if match:
                        col_name = match.group(1)
                        # 去掉表名前缀
                        col_name = col_name.split('.')[-1] if '.' in col_name else col_name
                        col_names.append(col_name)
                    else:
                        col_names.append(col.split('.')[-1] if '.' in col else col)
                elif ' AS ' in col:
                    base_part, alias = col.split(' AS ', 1)
                    col_names.append(alias)
                elif '(' in col and ')' in col:
                    col_names.append(col.split('(')[0])
                else:
                    col_names.append(col)
            elif hasattr(col, 'alias') and col.alias:
                col_names.append(col.alias)
            else:
                col_names.append(str(col))
        for name in col_names:
            table.add_column(name)
        row_count = 0
        for row in execution_result:
            if len(row) >= 2:
                row_id, row_data = row[0], row[1]
                formatted_data = []
                for value in row_data:
                    if isinstance(value, float):
                        if value.is_integer():
                            value = int(value)
                        else:
                            value = round(value, 2)
                    formatted_data.append(str(value))
                while len(formatted_data) < len(col_names):
                    formatted_data.append("")
                table.add_row(*formatted_data[:len(col_names)])
                row_count += 1
        console.print(table)
        console.print(f"[bold green]({row_count} rows)[/bold green]")
    
    def _display_show_tables_result(self, execution_result):
        """显示SHOW TABLES的结果（使用Rich）"""
        if not execution_result:
            console.print("[bold yellow]查询结果: 无数据[/bold yellow]")
            return
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Tables_in_database")
        row_count = 0
        if isinstance(execution_result, list):
            if execution_result and isinstance(execution_result[0], tuple) and len(execution_result[0]) == 2:
                for i, (row_id, (table_name,)) in enumerate(execution_result, 1):
                    table.add_row(str(table_name))
                    row_count += 1
            else:
                for i, item in enumerate(execution_result, 1):
                    table.add_row(str(item))
                    row_count += 1
        else:
            table.add_row(str(execution_result))
            row_count += 1
        console.print(table)
        console.print(f"[bold green]({row_count} rows)[/bold green]")

    def _display_query_result(self, execution_result, operator_tree):
        """显示查询结果为表格格式（使用Rich）"""
        if not execution_result:
            console.print("[bold yellow]查询结果: 无数据[/bold yellow]")
            return
        
        # 检查是否是聚合查询
        if self._is_aggregate_query(operator_tree):
            self._display_aggregate_result(execution_result, operator_tree)
            return
        # 获取表名和列信息
        table_name = None
        columns = []
        def find_scan_table(tree):
            if tree["type"] in ["SCAN", "INDEX_SCAN"] and "properties" in tree:
                return tree["properties"].get("table_name")
            elif "children" in tree and tree["children"]:
                for child in tree["children"]:
                    result = find_scan_table(child)
                    if result:
                        return result
            return None
        table_name = find_scan_table(operator_tree)
        def find_project_columns(tree):
            if tree["type"] == "PROJECT" and "properties" in tree:
                return tree["properties"].get("columns", [])
            elif "children" in tree and tree["children"]:
                for child in tree["children"]:
                    result = find_project_columns(child)
                    if result:
                        return result
            return []
        project_columns = find_project_columns(operator_tree)
        if project_columns:
            columns = []
            for col in project_columns:
                if hasattr(col, 'column_name'):
                    col_name = col.column_name
                    if col_name == "*" or col_name.endswith(".*"):
                        if table_name and table_name in self.catalog_manager.tables:
                            table_info = self.catalog_manager.tables[table_name]
                            columns = [col.column_name for col in table_info.columns]
                        break
                    else:
                        columns.append(col_name)
                elif isinstance(col, str):
                    if col == "*" or col.endswith(".*"):
                        if table_name and table_name in self.catalog_manager.tables:
                            table_info = self.catalog_manager.tables[table_name]
                            columns = [col.column_name for col in table_info.columns]
                        break
                    else:
                        # 处理列名，支持AST节点格式和普通字符串格式
                        if 'Identifier(' in col:
                            # 处理AST节点格式：Identifier(token=('ID', 'name', 0, 0), value='name')
                            import re
                            match = re.search(r"value='([^']+)'", col)
                            if match:
                                col_name = match.group(1)
                            else:
                                col_name = col.split('.')[-1] if '.' in col else col
                        else:
                            # 普通字符串格式，去掉表名前缀，如 'teachers.name' -> 'name'
                            col_name = col.split('.')[-1] if '.' in col else col
                        columns.append(col_name)
                else:
                    columns.append(str(col))
        if not columns and table_name and table_name in self.catalog_manager.tables:
            table_info = self.catalog_manager.tables[table_name]
            columns = [col.column_name for col in table_info.columns]
        # Rich表格输出
        table = Table(show_header=True, header_style="bold cyan")
        for col in columns:
            table.add_column(str(col))
        row_count = 0
        for row in execution_result:
            if len(row) >= 2:
                row_id, row_data = row[0], row[1]
                formatted_data = []
                for value in row_data:
                    if isinstance(value, bytes):
                        try:
                            decoded = value.decode('utf-8').rstrip('\x00')
                            formatted_data.append(decoded)
                        except:
                            formatted_data.append(str(value))
                    else:
                        formatted_data.append(str(value))
                while len(formatted_data) < len(columns):
                    formatted_data.append("")
                table.add_row(*formatted_data[:len(columns)])
                row_count += 1
        console.print(table)
        console.print(f"[bold green]({row_count} rows)[/bold green]")
    
    def _sync_table_to_symbol_table(self, physical_plan):
        """将CREATE TABLE的结果同步到symbol_table"""
        try:
            if hasattr(physical_plan, 'table_name') and hasattr(physical_plan, 'columns'):
                from src.sql_compiler.symbol_table import TableInfo, ColumnInfo, DataType
                table_name = physical_plan.table_name
                columns = physical_plan.columns
                # 检查表是否已存在
                if self.symbol_table.table_exists(table_name):
                    logger.info(f"Table '{table_name}' already exists in symbol table, skipping sync.")
                    return
                # 转换列信息为SymbolTable格式
                column_infos = []
                for col in columns:
                    col_name, col_type = col
                    # 转换数据类型
                    data_type = DataType.UNKNOWN
                    if col_type.upper() in ['INT', 'INTEGER']:
                        data_type = DataType.INT
                    elif col_type.upper() in ['VARCHAR', 'CHAR', 'TEXT']:
                        data_type = DataType.VARCHAR
                    elif col_type.upper() in ['DECIMAL', 'FLOAT', 'DOUBLE']:
                        data_type = DataType.DECIMAL
                    elif col_type.upper() == 'DATE':
                        data_type = DataType.DATE
                    elif col_type.upper() == 'TIMESTAMP':
                        data_type = DataType.TIMESTAMP
                    column_infos.append(ColumnInfo(col_name, data_type))
                table_info = TableInfo(table_name, column_infos)
                self.symbol_table.add_table(table_info)
        except Exception as e:
            logger.error(f"同步表到符号表失败: {e}")
    
    def _remove_table_from_symbol_table(self, table_name: str):
        """从符号表中移除表"""
        try:
            if self.symbol_table.table_exists(table_name):
                self.symbol_table.remove_table(table_name)
                print(f"Table '{table_name}' 已从符号表中移除")
        except Exception as e:
            logger.error(f"从符号表移除表失败: {e}")
    
    def read_multiline_sql(self):
        """读取多行SQL语句"""
        lines = []
        prompt = f"aodsql ({self.system_manager.current_db_name or 'no db'})> "
        print(prompt, end="", flush=True)
       
        while True:
            try:
                line = input()
                if not line.strip():
                    # 空行，检查是否结束
                    if lines and lines[-1].strip().endswith(';'):
                        break
                    continue
                
                # 检查退出命令
                if line.strip().lower() in ['quit', 'exit', 'q']:
                    return line.strip()
                
                # 清理输入：移除不可见字符，标准化引号
                cleaned_line = self._clean_sql_input(line)
                lines.append(cleaned_line)
                
                # 检查是否以分号结尾
                if cleaned_line.strip().endswith(';'):
                    break
                    
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                return None
            except EOFError:
                if lines:
                    break
                else:
                    print("\n\nGoodbye!")
                    return None
        
        return '\n'.join(lines).strip()
    
    def read_multiline_sql_from_file(self, file_content: str):
        """从文件内容读取多行SQL语句"""
        # 移除注释行
        lines = file_content.strip().split('\n')
        cleaned_lines = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('--'):
                cleaned_lines.append(line)
        
        # 重新组合内容
        content = '\n'.join(cleaned_lines)
        
        # 简单的按分号分割，不考虑字符串中的分号
        # 因为触发器语句中的分号都在字符串外面
        statements = content.split(';')
        
        sql_statements = []
        for statement in statements:
            statement = statement.strip()
            if statement:
                # 确保语句以分号结尾
                if not statement.endswith(';'):
                    statement += ';'
                sql_statements.append(statement)
        
        return sql_statements
    
    def _clean_sql_input(self, line):
        """清理SQL输入：移除不可见字符，标准化引号"""
        # 移除不可见字符（除了正常的空白字符）
        import re
        cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', line)
        
        # 标准化引号（将各种引号统一为标准引号）
        # 中文引号 -> 英文引号
        cleaned = cleaned.replace('"', '"').replace('"', '"')
        cleaned = cleaned.replace(''', "'").replace(''', "'")
        
        # 其他特殊引号 -> 标准引号
        cleaned = cleaned.replace('`', "'")
        
        return cleaned

    def run(self):
        """运行CLI主循环"""
        self.print_welcome()
        
        while True:
            try:
                sql_input = self.read_multiline_sql()
                
                if sql_input is None:
                    break
                
                if not sql_input:
                    continue
                
                # 退出条件
                if sql_input.lower() in ['quit', 'exit', 'q']:
                    print("Goodbye!")
                    break
                
                # 帮助命令
                if sql_input.lower() == 'help':
                    self.print_help()
                    continue
                
                # EXPLAIN ANALYZE命令
                if sql_input.upper().startswith('EXPLAIN ANALYZE'):
                    self.handle_explain_analyze(sql_input)
                    continue
                
                # 处理SQL输入
                if not self.process_sql_input(sql_input):
                    break
                
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"❌ System error: {str(e)}")
                import traceback
                traceback.print_exc()
    
    def _print_optimization_info(self, optimization_info: Dict[str, Any]):
        """打印优化信息"""
        logger.info("查询优化信息：")
        print("=" * 40)
        if optimization_info.get("optimization_applied", False):
            logger.info("已应用优化。")
            original_cost = optimization_info.get("original_cost", 0)
            optimized_cost = optimization_info.get("optimized_cost", 0)
            if original_cost > 0 and optimized_cost > 0:
                improvement = ((original_cost - optimized_cost) / original_cost) * 100
                logger.info(f"成本优化：原始成本: {original_cost:.2f}，优化后成本: {optimized_cost:.2f}，性能提升: {improvement:.1f}%")
            decisions = optimization_info.get("decisions", [])
            if decisions:
                logger.info(f"优化决策（共{len(decisions)}项）：")
                for i, decision in enumerate(decisions, 1):
                    logger.info(f"  {i}. 表: {decision.get('table', 'N/A')}")
                    if decision.get('chosen'):
                        logger.info(f"    选择索引: {decision.get('index_name', 'N/A')}")
                        logger.info(f"    选择性: {decision.get('selectivity_estimate', 0):.3f}")
                    else:
                        logger.info(f"    使用顺序扫描，顺序扫描成本: {decision.get('seq_cost_estimate', 0):.2f}")
        else:
            logger.warning("未应用优化。")
            reason = optimization_info.get("reason", "未知原因")
            logger.warning(f"原因: {reason}")
            if optimization_info.get("fallback_to_original", False):
                error = optimization_info.get("error", "未知错误")
                logger.error(f"错误: {error}")
                logger.warning("已使用原始执行计划。")
            print("-" * 50)
    
    def _is_cursor_command(self, physical_plan) -> bool:
        """检查是否是游标命令"""
        from src.engine.operator import DeclareCursor, OpenCursor, FetchCursor, CloseCursor
        return isinstance(physical_plan, (DeclareCursor, OpenCursor, FetchCursor, CloseCursor))
    
    def _handle_cursor_command(self, physical_plan) -> str:
        """处理游标命令"""
        from src.engine.operator import DeclareCursor, OpenCursor, FetchCursor, CloseCursor

        # 设置CLI接口引用
        physical_plan.cli_interface = self
        
        if isinstance(physical_plan, DeclareCursor):
            return self._handle_declare_cursor(physical_plan)
        elif isinstance(physical_plan, OpenCursor):
            return self._handle_open_cursor(physical_plan)
        elif isinstance(physical_plan, FetchCursor):
            return self._handle_fetch_cursor(physical_plan)
        elif isinstance(physical_plan, CloseCursor):
            return self._handle_close_cursor(physical_plan)
        else:
            return "❌ Unknown cursor command type"
    
    def _handle_declare_cursor(self, physical_plan) -> str:
        """处理DECLARE CURSOR命令"""
        cursor_name = physical_plan.cursor_name
        if cursor_name in self.cursors:
            return f"❌ Error: Cursor '{cursor_name}' already exists."
        
        # 创建游标信息并存储
        cursor_info = CursorInfo(
            name=cursor_name,
            plan=physical_plan.query_plan,
            status=CursorStatus.DECLARED
        )
        self.cursors[cursor_name] = cursor_info
        return f"✅ Cursor '{cursor_name}' declared."
    
    def _handle_open_cursor(self, physical_plan) -> str:
        """处理OPEN CURSOR命令"""
        cursor_name = physical_plan.cursor_name
        if cursor_name not in self.cursors:
            return f"❌ Error: Cursor '{cursor_name}' does not exist."
        
        self.cursors[cursor_name].status = CursorStatus.OPEN
        return f"✅ Cursor '{cursor_name}' opened."
    
    def _handle_fetch_cursor(self, physical_plan) -> str:
        """处理FETCH CURSOR命令"""
        cursor_name = physical_plan.cursor_name
        cursor_info = self.cursors.get(cursor_name)
        if not cursor_info or cursor_info.status != CursorStatus.OPEN:
            return f"❌ Error: Cursor '{cursor_name}' not opened or does not exist."
        
        # 从游标的查询计划中获取下一批数据
        batch = cursor_info.plan.next()
        if batch:
            # 显示结果
            if hasattr(self, 'display') and self.display:
                self.display.display_results(batch, cursor_info.plan.schema)
            else:
                # 简单的结果显示
                for row_id, row_data in batch:
                    print(f"Row {row_id}: {row_data}")
            return f"✅ Fetched {len(batch)} rows from cursor '{cursor_name}'."
        else:
            return "(No more rows)"
    
    def _handle_close_cursor(self, physical_plan) -> str:
        """处理CLOSE CURSOR命令"""
        cursor_name = physical_plan.cursor_name
        if cursor_name in self.cursors:
            del self.cursors[cursor_name]
            return f"✅ Cursor '{cursor_name}' closed."
        else:
            return f"❌ Error: Cursor '{cursor_name}' does not exist."
    
    def _is_transaction_command(self, physical_plan) -> bool:
        """检查是否是事务命令"""
        if hasattr(physical_plan, 'type'):
            return physical_plan.type in ['BEGIN_TRANSACTION', 'COMMIT_TRANSACTION', 'ROLLBACK_TRANSACTION']
        return False
    
    def _handle_transaction_command(self, physical_plan) -> str:
        """处理事务命令"""
        if not self.transaction_manager:
            return "❌ Error: Transaction manager not initialized"
        
        if physical_plan.type == 'BEGIN_TRANSACTION':
            # 开始新事务
            if self._is_manual_transaction and self._current_transaction:
                return "❌ Error: Active transaction already exists, please commit or rollback first"
            self._current_transaction = self.transaction_manager.begin()
            self._is_manual_transaction = True
            return f"✅ Manual transaction started (Transaction ID: {self._current_transaction.id})"
        
        elif physical_plan.type == 'COMMIT_TRANSACTION':
            # 提交事务
            if not self._is_manual_transaction or not self._current_transaction:
                return "❌ Error: No active manual transaction to commit"
            success = self.transaction_manager.commit(self._current_transaction)
            self._current_transaction = None
            self._is_manual_transaction = False
            return "✅ Manual transaction committed" if success else "❌ Manual transaction commit failed"
        
        elif physical_plan.type == 'ROLLBACK_TRANSACTION':
            # 回滚事务
            if not self._is_manual_transaction or not self._current_transaction:
                return "❌ Error: No active manual transaction to rollback"
            success = self.transaction_manager.abort(self._current_transaction)
            self._current_transaction = None
            self._is_manual_transaction = False
            return "✅ Manual transaction rolled back" if success else "❌ Manual transaction rollback failed"
        
        return "❌ Error: Unknown transaction command"
