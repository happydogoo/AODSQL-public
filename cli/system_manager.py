# system_manager.py (REFACTORED)

import os
import shutil
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from typing import Dict, Optional, Any

from src.engine.executor import Executor
from src.engine.storage.real_storage_engine import RealStorageEngine
from src.engine.catalog_manager import CatalogManager
from src.sql_compiler.sql_interpreter import SQLInterpreter
from src.sql_compiler.symbol_table import SymbolTable
from src.engine.transaction.log_manager import LogManager
from src.engine.transaction.lock_manager import LockManager
from src.engine.transaction.transaction_manager import TransactionManager

class SystemManager:
    """系统管理类，负责管理多个数据库实例的生命周期"""
    
    def __init__(self, base_data_dir: str = 'data'):
        self.base_data_dir = base_data_dir
        self.databases: Dict[str, Dict] = {}  # db_name -> { 'catalog_manager': ..., 'storage_engine': ... }
        self.current_db_name: Optional[str] = None
        
        if not os.path.exists(self.base_data_dir):
            os.makedirs(self.base_data_dir)
        

        self._load_existing_databases()

    def _load_existing_databases(self):
        """启动时扫描数据目录，加载已存在的数据库"""
        for db_name in os.listdir(self.base_data_dir):
            db_path = os.path.join(self.base_data_dir, db_name)
            if os.path.isdir(db_path):
                # 注意：这里只记录存在，不立即加载，实现懒加载
                # 实际加载将在第一次 use_database 时发生
                pass
        
    def create_database(self, db_name: str):
        """创建一个新的数据库实例"""
        if db_name in os.listdir(self.base_data_dir):
            raise Exception(f"数据库 '{db_name}' 已存在。")

        db_path = os.path.join(self.base_data_dir, db_name)
        os.makedirs(db_path, exist_ok=True)
        
        # 为新数据库创建一套完整的、独立的组件
        # 注意：CatalogManager 现在需要接收一个文件路径
        catalog = CatalogManager(catalog_path=os.path.join(db_path, 'catalog.json'))
        lock_manager = LockManager()
        # LogManager 和 RealStorageEngine 也使用该数据库专属的路径
        log_manager = LogManager(log_file_path=os.path.join(db_path, 'db.log'), storage_engine=None)
        storage_engine = RealStorageEngine(catalog, log_manager, lock_manager, data_dir=db_path)
        
        # 关键一步：将 storage_engine 实例回填给 log_manager，以解决循环依赖
        log_manager._storage_engine = storage_engine
        
        self.databases[db_name] = {
            "catalog_manager": catalog,
            "storage_engine": storage_engine,
            "log_manager": log_manager,
            "lock_manager": lock_manager,
            "symbol_table": SymbolTable(),
            "sql_interpreter": SQLInterpreter(SymbolTable(), catalog),
            "executor": Executor(storage_engine, catalog),
            "transaction_manager": TransactionManager(lock_manager, log_manager)
        }

    def drop_database(self, db_name: str):
        """删除一个数据库及其所有文件"""
        if db_name not in os.listdir(self.base_data_dir):
            raise Exception(f"数据库 '{db_name}' 不存在。")
            
        # 如果数据库已加载在内存中，先清理
        if db_name in self.databases:
            # 可以在这里调用 storage_engine.close_all() 等清理方法
            del self.databases[db_name]
        
        db_path = os.path.join(self.base_data_dir, db_name)
        shutil.rmtree(db_path)

    def use_database(self, db_name: str):
        """切换到指定的数据库上下文"""
        db_path = os.path.join(self.base_data_dir, db_name)
        if not os.path.isdir(db_path):
            raise Exception(f"数据库 '{db_name}' 不存在。")
        
        # 切换到新数据库时，重新加载组件以确保使用正确的catalog_manager
        self.current_db_name = db_name
        # 强制重新加载数据库组件
        if db_name in self.databases:
            del self.databases[db_name]

    def get_current_components(self) -> Dict:
        """获取当前数据库上下文的全套组件"""
        if self.current_db_name is None:
            raise Exception("错误：未选择任何数据库。请先使用 'USE database_name;' 命令。")


        # 实现懒加载：如果组件还未加载到内存，现在进行加载
        if self.current_db_name not in self.databases:
            self._load_database_components(self.current_db_name)
            
        return self.databases[self.current_db_name]

    def _load_database_components(self, db_name: str):
        """加载指定数据库的组件到内存"""
        db_path = os.path.join(self.base_data_dir, db_name)
        
        catalog = CatalogManager(catalog_path=os.path.join(db_path, 'catalog.json'))
        lock_manager = LockManager()
        log_manager = LogManager(log_file_path=os.path.join(db_path, 'db.log'), storage_engine=None)
        storage_engine = RealStorageEngine(catalog, log_manager, lock_manager, data_dir=db_path)
        log_manager._storage_engine = storage_engine
        
        # 为该数据库创建独立的编译器和执行器
        symbol_table = SymbolTable()
        # 从该数据库的 catalog 加载表到符号表
        for table_name, table_info in catalog.tables.items():
            # ... 此处省略将ColumnInfo转换为SymbolTable格式的代码 ...
            pass
            
        sql_interpreter = SQLInterpreter(symbol_table, catalog)
        executor = Executor(storage_engine, catalog)
        transaction_manager = TransactionManager(lock_manager, log_manager)

        self.databases[db_name] = {
            "catalog_manager": catalog,
            "storage_engine": storage_engine,
            "log_manager": log_manager,
            "lock_manager": lock_manager,
            "symbol_table": symbol_table,
            "sql_interpreter": sql_interpreter,
            "executor": executor,
            "transaction_manager": transaction_manager
        }

    def initialize_system(self):
        """初始化系统，创建默认数据库并切换到默认库"""
        if 'default' not in self.databases:
            self.create_database('default')
        self.use_database('default')
        return self

    def initialize_sample_data(self):
        """在默认数据库中创建示例表并插入数据"""
        self.use_database('default')
        comps = self.get_current_components()
        catalog_manager = comps['catalog_manager']
        storage_engine = comps['storage_engine']
        symbol_table = comps['symbol_table']
        # 创建示例表
        table_name = 'employees'
        schema = [('id', 'INT'), ('name', 'VARCHAR(50)'), ('age', 'INT')]
        try:
            if not hasattr(catalog_manager, 'tables') or table_name not in catalog_manager.tables:
                # 事务对象可为None或模拟
                storage_engine.create_table(None, table_name, schema)
            # 插入示例数据
            rows = [
                (1, 'Alice', 30),
                (2, 'Bob', 25),
                (3, 'Charlie', 28)
            ]
            for row in rows:
                storage_engine.insert_row(None, table_name, row)
        except Exception as e:
            raise e

    def execute_sql_statement(self, sql: str) -> Dict[str, Any]:
        """
        执行一条SQL语句并返回结构化的结果。
        这是专门为GUI或其他程序化调用设计的。
        """
        if not self.current_db_name:
            return {'type': 'ERROR', 'message': '没有选择数据库。请先使用 USE DATABASE 命令。'}
        
        try:
            # 1. 获取当前数据库的组件
            components = self.get_current_components()
            sql_interpreter = components['sql_interpreter']
            catalog_manager = components['catalog_manager']
            storage_engine = components['storage_engine']
            executor = components['executor']
            transaction_manager = components['transaction_manager']
            
            # 2. 解析SQL
            result = sql_interpreter.interpret(sql)
            
            if result["status"] == "error":
                return {'type': 'ERROR', 'message': result['message']}
            
            # 3. 将AST转换为执行计划
            from cli.plan_converter import PlanConverter
            plan_converter = PlanConverter(storage_engine, catalog_manager)
            physical_plan = plan_converter.convert_to_physical_plan(result["operator_tree"])
            
            if not physical_plan:
                return {'type': 'ERROR', 'message': '无法生成物理执行计划'}
            
            # 4. 执行计划
            if transaction_manager:
                transaction = transaction_manager.begin()
                try:
                    execution_result = executor.execute_plan(physical_plan, transaction)
                    transaction_manager.commit(transaction)
                    # 事务提交后保存catalog
                    catalog_manager._save_catalog()
                except Exception as e:
                    transaction_manager.abort(transaction)
                    raise e
            else:
                # 如果没有事务管理器，尝试创建一个简单的事务对象
                from src.engine.transaction.transaction import Transaction, IsolationLevel
                transaction = Transaction(1, IsolationLevel.READ_COMMITTED)
                execution_result = executor.execute_plan(physical_plan, transaction)
                # 执行后保存catalog
                catalog_manager._save_catalog()

            # 5. 根据计划类型格式化返回结果
            operator_tree = result["operator_tree"]
            plan_type = operator_tree["type"].upper()

            if plan_type in ["SELECT", "PROJECT", "ORDER_BY", "AGGREGATE", "LIMIT", "SORT"]:
                # 查询结果
                if not execution_result:
                    return {'type': 'SELECT', 'headers': [], 'rows': []}
                
                # 提取列信息
                headers = self._extract_headers_from_result(execution_result, operator_tree, catalog_manager)
                
                # 提取行数据
                rows = []
                for row in execution_result:
                    if len(row) >= 2:
                        row_id, row_data = row[0], row[1]
                        # 格式化数据
                        formatted_row = self._format_row_data(row_data)
                        rows.append(formatted_row)
                
                return {'type': 'SELECT', 'headers': headers, 'rows': rows}
            
            elif plan_type == "SHOW_TABLES":
                # SHOW TABLES 结果
                if not execution_result:
                    return {'type': 'SELECT', 'headers': ['Tables'], 'rows': []}
                
                headers = ['Tables']
                rows = []
                for row in execution_result:
                    if len(row) >= 2:
                        row_id, row_data = row[0], row[1]
                        if isinstance(row_data, (list, tuple)) and len(row_data) > 0:
                            rows.append([row_data[0]])
                        else:
                            rows.append([str(row_data)])
                
                return {'type': 'SELECT', 'headers': headers, 'rows': rows}
            
            elif plan_type in ["INSERT", "UPDATE", "DELETE"]:
                # DML操作结果
                rows_affected = len(execution_result) if execution_result else 0
                return {'type': 'DML', 'message': f'{rows_affected} 行受影响。'}

            elif plan_type in ["CREATE_TABLE", "DROP_TABLE"]:
                # DDL操作结果
                return {'type': 'DDL', 'message': '操作成功。'}
            
            else:
                return {'type': 'UNKNOWN', 'message': '执行完成，但未识别操作类型。'}

        except Exception as e:
            # 捕获解析、转换或执行过程中的任何错误
            import traceback
            return {'type': 'ERROR', 'message': str(e)}
    
    def _extract_headers_from_result(self, execution_result, operator_tree, catalog_manager):
        """从执行结果中提取列头信息"""
        # 检查是否是PROJECT操作，如果是则使用指定的列
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
            # 处理列名，去掉表名前缀
            headers = []
            for col in project_columns:
                if hasattr(col, 'column_name'):
                    col_name = col.column_name
                    if col_name == "*" or col_name.endswith(".*"):
                        # SELECT * 的情况，需要从catalog获取所有列
                        table_name = self._find_scan_table(operator_tree)
                        if table_name and table_name in catalog_manager.tables:
                            table_info = catalog_manager.tables[table_name]
                            headers = [col.column_name for col in table_info.columns]
                        break
                    else:
                        headers.append(col_name)
                elif isinstance(col, str):
                    if col == "*" or col.endswith(".*"):
                        # SELECT * 的情况
                        table_name = self._find_scan_table(operator_tree)
                        if table_name and table_name in catalog_manager.tables:
                            table_info = catalog_manager.tables[table_name]
                            headers = [col.column_name for col in table_info.columns]
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
                        headers.append(col_name)
                else:
                    headers.append(str(col))
            return headers
        
        # 如果没有找到列信息，从catalog获取所有列
        table_name = self._find_scan_table(operator_tree)
        if table_name and table_name in catalog_manager.tables:
            table_info = catalog_manager.tables[table_name]
            return [col.column_name for col in table_info.columns]
        
        # 如果还是没有找到，从数据中推断
        if execution_result and len(execution_result) > 0:
            first_row = execution_result[0]
            if len(first_row) >= 2:
                row_data = first_row[1]
                if isinstance(row_data, (list, tuple)):
                    return [f"col_{i+1}" for i in range(len(row_data))]
        
        return []
    
    def _find_scan_table(self, tree):
        """递归查找SCAN操作中的表名"""
        if tree["type"] in ["SCAN", "INDEX_SCAN"] and "properties" in tree:
            return tree["properties"].get("table_name")
        elif "children" in tree and tree["children"]:
            for child in tree["children"]:
                result = self._find_scan_table(child)
                if result:
                    return result
        return None
    
    def _format_row_data(self, row_data):
        """格式化行数据"""
        formatted_data = []
        for value in row_data:
            if isinstance(value, bytes):
                # 解码字节数据
                try:
                    decoded = value.decode('utf-8').rstrip('\x00')
                    
                    # 处理双重序列化的情况
                    if decoded.startswith("b'") and decoded.endswith("'"):
                        inner = decoded[2:-1]
                        inner = inner.replace('\\x00', '').replace('\\n', '\n').replace('\\t', '\t')
                        if inner.startswith('"') and inner.endswith('"'):
                            inner = inner[1:-1]
                        elif inner.startswith("'") and inner.endswith("'"):
                            inner = inner[1:-1]
                        decoded = inner
                    
                    # 处理Unicode转义序列
                    elif '\\x' in decoded:
                        import codecs
                        try:
                            decoded = codecs.decode(decoded, 'unicode_escape')
                        except:
                            pass
                    
                    # 移除可能的引号包装
                    if decoded.startswith("'") and decoded.endswith("'"):
                        decoded = decoded[1:-1]
                    elif decoded.startswith('"') and decoded.endswith('"'):
                        decoded = decoded[1:-1]
                    
                    formatted_data.append(decoded)
                except:
                    formatted_data.append(str(value))
            else:
                formatted_data.append(str(value))
        
        return formatted_data

    def shutdown(self):
        """关闭系统，确保所有数据库数据持久化并安全关闭"""
        for db_name, comps in self.databases.items():
            try:
                storage_engine = comps.get('storage_engine')
                if storage_engine and hasattr(storage_engine, 'flush_all_tables'):
                    storage_engine.flush_all_tables()
                else:
                    buffer_pool = comps.get('buffer_pool')
                    if buffer_pool and hasattr(buffer_pool, 'flush_all'):
                        buffer_pool.flush_all()
            except Exception as e:
                print(f"⚠️  保存数据库 '{db_name}' 时出现错误: {e}")
            try:
                storage_engine = comps.get('storage_engine')
                if storage_engine and hasattr(storage_engine, 'close_all'):
                    storage_engine.close_all()
                tablespace_manager = comps.get('tablespace_manager')
                if tablespace_manager and hasattr(tablespace_manager, 'close'):
                    tablespace_manager.close()
            except Exception as e:
                print(f"⚠️  关闭数据库 '{db_name}' 存储系统时出现错误: {e}")