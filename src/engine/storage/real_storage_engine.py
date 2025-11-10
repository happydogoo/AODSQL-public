# In engine/real_storage_engine.py

from typing import Iterator, Tuple, Any, List, Optional
import os

from src.engine.storage.storage_engine import StorageEngine
from src.engine.catalog_manager import CatalogManager
from src.storage.buffer import BufferPool
from src.storage.tablespace_manager import TablespaceManager
from src.storage.page import BasePage
from src.engine.storage.tuple_serializer import TupleSerializer
from src.engine.storage.heap_file_manager import HeapFileManager
from src.engine.storage.index_manager import IndexManager
from src.engine.transaction.lock_manager import ResourceID, LockManager, LockMode
from src.engine.transaction.log_manager import LogManager, InsertLogRecord, DeleteLogRecord, UpdateLogRecord
from loguru import logger

class RealStorageEngine(StorageEngine):
    """
    存储引擎的真实实现。
    它通过协调 CatalogManager, BufferPool 和 TablespaceManager 来完成所有数据操作。
    """
    def __init__(self, catalog_manager: CatalogManager, log_manager: LogManager, lock_manager: LockManager, data_dir: str = 'data', buffer_size: int = 16):
        """
        初始化真实的存储引擎。
        :param catalog_manager: 系统目录管理器，用于获取表的元数据。
        :param log_manager: 日志管理器。
        :param lock_manager: 锁管理器。
        :param data_dir: 存放所有表数据文件的目录。
        :param buffer_size: 每个表的缓冲池大小。
        """
        self.catalog_manager = catalog_manager
        self.log_manager = log_manager
        self.lock_manager = lock_manager
        self.data_dir = data_dir
        self.buffer_size = buffer_size
        self.tablespace_managers = {}  # 表名 -> TablespaceManager
        self.buffer_pools = {}         # 表名 -> BufferPool
        self.indexspace_managers = {}
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

    def _get_tablespace_and_buffer(self, table_name: str):
        if table_name not in self.tablespace_managers:
            table_info = self.catalog_manager.get_table(table_name)
            file_path = os.path.join(self.data_dir, table_info.file_name)
            tm = TablespaceManager(file_path)
            bp = BufferPool(tm, self.buffer_size, self.log_manager)
            self.tablespace_managers[table_name] = tm
            self.buffer_pools[table_name] = bp
        return self.tablespace_managers[table_name], self.buffer_pools[table_name]

    def _get_indexspace_and_buffer(self, table_name: str, index_name: str, key_col_types=None):
        """
        获取指定索引的TablespaceManager和BufferPool，若不存在则创建。
        """
        table_info = self.catalog_manager.get_table(table_name)
        idx_info = table_info.indexes[index_name]

        file_path = os.path.join(self.data_dir, idx_info.file_name)
        print("_get_indexspace_and_buffer",file_path)
        key = f"{table_name}::{index_name}"
        if not hasattr(self, 'indexspace_managers'):
            self.indexspace_managers = {}
        if not hasattr(self, 'index_buffer_pools'):
            self.index_buffer_pools = {}
        if key not in self.indexspace_managers:
            from src.storage.tablespace_manager import TablespaceManager
            tm = TablespaceManager(file_path)
            from src.storage.buffer import BufferPool
            bp = BufferPool(tm, self.buffer_size, self.log_manager)
            self.indexspace_managers[key] = tm
            self.index_buffer_pools[key] = bp
        return self.indexspace_managers[key], self.index_buffer_pools[key]

    def create_table(self, transaction, table_name: str, schema: List[Tuple[str, str]]):
        """
        创建一个新表。
        1. 在系统目录中注册表。
        2. 在物理层面创建对应的表空间/文件（由TablespaceManager处理）。
        3. 自动为主键创建唯一索引。
        """
        # 对目录加排他锁
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        # 写日志（使用专用DDL日志类型）
        from src.engine.transaction.log_manager import CreateTableLogRecord
        log_record = CreateTableLogRecord(transaction.id, table_name, schema)
        self.log_manager.append(transaction, log_record)
        file_name = f"{table_name}.db"
        self.catalog_manager.create_table(transaction, table_name, schema, file_name=file_name)
        file_path = os.path.join(self.data_dir, file_name)
        tm = TablespaceManager(file_path)
        bp = BufferPool(tm, self.buffer_size, self.log_manager)
        self.tablespace_managers[table_name] = tm
        self.buffer_pools[table_name] = bp
        
        # 处理主键约束 - 自动创建主键索引
        # 注意：这里需要先设置主键信息，然后再创建索引
        # 主键信息应该在create_table之前设置，但这里我们暂时跳过
        self._create_primary_key_index(transaction, table_name)

    def insert_row(self, transaction, table_name: str, row: Tuple) -> Any:
        """向表中插入一条新记录。"""
        table_info = self.catalog_manager.get_table(table_name)
        schema = table_info.columns
        serializer = TupleSerializer(schema)
        tm, bp = self._get_tablespace_and_buffer(table_name)
        heap_file = HeapFileManager(bp, table_info, self.catalog_manager)
        # 构造索引BufferPool字典
        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = self._get_indexspace_and_buffer(table_name, index_name, table_info.indexes[index_name].key_col_types)
            index_buffer_pools[index_name] = idx_bp
        index_manager = IndexManager(table_info, bp, index_buffer_pools, self.catalog_manager)
        # 约束检查和默认值处理
        checked_row = list(row)
        for i, col in enumerate(schema):
            if hasattr(col, 'column_name'):
                not_null = getattr(col, 'not_null', False)
                default = getattr(col, 'default', None)
                check_expr = getattr(col, 'check', None)
                is_primary_key = getattr(col, 'is_primary_key', False)
                col_name = col.column_name
            else:
                not_null = False
                default = None
                check_expr = None
                is_primary_key = False
                col_name = col[0]
            
            # 主键约束检查
            if is_primary_key:
                if checked_row[i] is None or checked_row[i] == '':
                    raise ValueError(f"Primary key column '{col_name}' cannot be NULL")
                
                # 检查主键唯一性
                primary_key_value = checked_row[i]
                existing_row_id = self._check_primary_key_uniqueness(
                    transaction, table_name, col_name, primary_key_value
                )
                if existing_row_id is not None:
                    raise ValueError(f"Primary key value '{primary_key_value}' already exists")
            
            # 其他约束检查
            if (checked_row[i] is None or checked_row[i] == '') and not_null:
                if default is not None:
                    checked_row[i] = default
                else:
                    raise ValueError(f"Column '{col_name}' cannot be NULL")
            elif (checked_row[i] is None or checked_row[i] == '') and default is not None:
                checked_row[i] = default
            if check_expr:
                context = {col_name: checked_row[i]}
                if not eval(check_expr, {}, context):
                    raise ValueError(f"CHECK constraint failed for column '{col_name}': {check_expr}")
        row = tuple(checked_row)
        row_bytes = serializer.serialize(row)
        try:
            # --- 事务化核心逻辑 ---
            # 1. 预定位，获取将要插入的 row_id（不做物理写）
            row_id = heap_file.find_space_for_record(row_bytes)  # 你需确保此方法返回(page_id, record_id)
            page_id, record_id = row_id
            if page_id is not None:
                self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID(table_name, page_id))
            # 2. 写数据日志（InsertLogRecord）
            log_record = InsertLogRecord(transaction.id, ResourceID(table_name, page_id, record_id), row_bytes)
            lsn = self.log_manager.append(transaction, log_record)
            # 3. 物理插入（日志先行）
            heap_file.insert_record_at(transaction, row_id, row_bytes, lsn)
            # 4. 更新索引（为每个索引生成独立lsn）
            lsn_map = {index_name: lsn for index_name in table_info.indexes}
            index_manager.insert_entry(transaction, row, row_id, lsn_map)
            # 5. 更新目录统计
            self.catalog_manager.inc_row_count(transaction, table_name, 1)
            return row_id
        except Exception:
            raise

    def scan(self, transaction, table_name: str) -> Iterator[Tuple[Any, Tuple]]:
        """以全表扫描的方式读取所有记录。通过 root_page_id 和 next_page_id 链表遍历。"""
        table_info = self.catalog_manager.get_table(table_name)
        schema = table_info.columns
        serializer = TupleSerializer(schema)
        tm, bp = self._get_tablespace_and_buffer(table_name)
        heap_file = HeapFileManager(bp, table_info, self.catalog_manager)
        # 加表级共享锁
        self.lock_manager.acquire(transaction, LockMode.SHARED, ResourceID(table_name))
        for row_id, row_bytes in heap_file.scan(transaction, serializer.get_record_size()):
            row_tuple = serializer.deserialize(row_bytes)
            yield (row_id, row_tuple)
    
    def update_row(self, transaction, table_name: str, row_id: Any, new_row: Tuple):
        """更新一条指定的记录。"""
        if table_name not in self.tablespace_managers:
            raise Exception(f"Table {table_name} not found")
        table_info = self.catalog_manager.get_table(table_name)
        schema = table_info.columns
        serializer = TupleSerializer(schema)
        tm, bp = self._get_tablespace_and_buffer(table_name)
        heap_file = HeapFileManager(bp, table_info, self.catalog_manager)
        # 构造索引BufferPool字典
        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = self._get_indexspace_and_buffer(table_name, index_name, table_info.indexes[index_name].key_col_types)
            index_buffer_pools[index_name] = idx_bp
        index_manager = IndexManager(table_info, bp, index_buffer_pools, self.catalog_manager)
        record_size = serializer.get_record_size()
        page_id, record_no = row_id
        # 先加锁，再读取旧数据，防止丢失更新
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID(table_name, page_id, record_no))
        old_row_bytes = heap_file.get_record(transaction, row_id, record_size)
        old_row = serializer.deserialize(old_row_bytes)
        
        # 主键约束检查（如果更新了主键列）
        for i, col in enumerate(schema):
            if hasattr(col, 'is_primary_key') and getattr(col, 'is_primary_key', False):
                if new_row[i] is None or new_row[i] == '':
                    raise ValueError(f"Primary key column '{col.column_name}' cannot be NULL")
                
                # 检查主键唯一性（排除当前行）
                primary_key_value = new_row[i]
                existing_row_id = self._check_primary_key_uniqueness(
                    transaction, table_name, col.column_name, primary_key_value, exclude_row_id=row_id
                )
                if existing_row_id is not None:
                    raise ValueError(f"Primary key value '{primary_key_value}' already exists")
        
        new_row_bytes = serializer.serialize(new_row)
        log_record = UpdateLogRecord(transaction.id, ResourceID(table_name, page_id, record_no), old_row_bytes, new_row_bytes)
        lsn = self.log_manager.append(transaction, log_record)
        lsn_map = {index_name: lsn for index_name in table_info.indexes}
        index_manager.update_entries(transaction, old_row, new_row, row_id, lsn_map)
        heap_file.update_record(transaction, row_id, new_row_bytes, lsn)
        return row_id

    def delete_row(self, transaction, table_name: str, row_id: Any):
        """删除一条指定的记录。"""
        if table_name not in self.tablespace_managers:
            raise Exception(f"Table {table_name} not found")
        table_info = self.catalog_manager.get_table(table_name)
        schema = table_info.columns
        serializer = TupleSerializer(schema)
        tm, bp = self._get_tablespace_and_buffer(table_name)
        heap_file = HeapFileManager(bp, table_info, self.catalog_manager)
        # 构造索引BufferPool字典
        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = self._get_indexspace_and_buffer(table_name, index_name, table_info.indexes[index_name].key_col_types)
            index_buffer_pools[index_name] = idx_bp
        index_manager = IndexManager(table_info, bp, index_buffer_pools, self.catalog_manager)
        record_size = serializer.get_record_size()
        page_id, record_no = row_id
        # 先加锁，再读取旧数据，防止丢失更新
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID(table_name, page_id, record_no))
        old_row_bytes = heap_file.get_record(transaction, row_id, record_size)
        old_row = serializer.deserialize(old_row_bytes)
        log_record = DeleteLogRecord(transaction.id, ResourceID(table_name, page_id, record_no), old_row_bytes)
        lsn = self.log_manager.append(transaction, log_record)
        lsn_map = {index_name: lsn for index_name in table_info.indexes}
        index_manager.delete_entry(transaction, old_row, row_id, lsn_map)
        heap_file.delete_record(transaction, row_id, record_size, lsn)

# In class RealStorageEngine:

    def flush_all_tables(self):
        """
        【修复版】刷新所有表和索引的缓冲池到磁盘。
        """
        # 1. 刷新所有表的缓冲池
        for table_name, buffer_pool in self.buffer_pools.items():
            try:
                buffer_pool.flush_all()
                logger.info(f"表 '{table_name}' 数据已刷新到磁盘")
            except Exception as e:
                logger.error(f"刷新表 '{table_name}' 时出现错误: {e}")

        # 2. 【新增】刷新所有索引的缓冲池
        if hasattr(self, 'index_buffer_pools'):
            for index_key, buffer_pool in self.index_buffer_pools.items():
                try:
                    buffer_pool.flush_all()
                    logger.info(f"索引 '{index_key}' 数据已刷新到磁盘")
                except Exception as e:
                    logger.error(f"刷新索引 '{index_key}' 时出现错误: {e}")
    
    def close_all(self):
        """关闭所有表的TablespaceManager，释放文件句柄。"""
        self.flush_all_tables()
        # 然后关闭表空间管理器
        for table_name, tm in self.tablespace_managers.items():
            try:
                tm.close()
                logger.debug(f"[close_all] 表 '{table_name}' 的TablespaceManager已关闭。")
            except Exception as e:
                logger.debug(f"[close_all] 关闭表 '{table_name}' 时出错: {e}")
        if self.indexspace_managers:
            for key, tm in self.indexspace_managers.items():
                try:
                    tm.close()
                    logger.debug(f"[close_all] 索引空间 '{key}' 的TablespaceManager已关闭。")
                except Exception as e:
                    logger.debug(f"[close_all] 关闭索引空间 '{key}' 时出错: {e}")

    def close_table(self, table_name: str):
        """关闭指定表的TablespaceManager，释放文件句柄。"""
        tm = self.tablespace_managers.get(table_name)
        if tm:
            tm.close()
            del self.tablespace_managers[table_name]
        if table_name in self.buffer_pools:
            del self.buffer_pools[table_name]

    def drop_table_file(self, table_name: str):
        """删除指定表的所有数据页并删除物理文件。"""
        table_info = self.catalog_manager.get_table(table_name)
        file_path = os.path.join(self.data_dir, table_info.file_name)
        # 1. 递归回收所有数据页
        bp = self.buffer_pools.get(table_name)
        if bp and table_info.root_page_id is not None:
            bp.delete_table_pages(table_info.root_page_id)
        # 2. 通过TablespaceManager安全删除物理文件
        tm = self.tablespace_managers.get(table_name)
        if tm:
            tm.delete_file()
            del self.tablespace_managers[table_name]
        # elif os.path.exists(file_path):
        #     os.remove(file_path)

    def drop_table(self, transaction, table_name: str, if_exists: bool = False):
        """统一封装删除表的所有底层操作。"""
        # 检查表是否存在
        if not self.catalog_manager.table_exists(table_name):
            if if_exists:
                return f"Table '{table_name}' does not exist, skipping."
            else:
                raise Exception(f"Table '{table_name}' does not exist")
        
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import DropTableLogRecord
        log_record = DropTableLogRecord(transaction.id, table_name)
        self.log_manager.append(transaction, log_record)
        self.drop_table_file(table_name)
        self.close_table(table_name)
        self.catalog_manager.delete_table(transaction, table_name)
        return f"Table '{table_name}' dropped."

    def delete_all_tables(self):
        """
        删除所有表及其物理文件。
        """
        table_names = list(self.catalog_manager.list_tables())
        for name in table_names:
            try:
                self.drop_table(name)
            except Exception:
                pass

    def create_index(self, transaction, table_name: str, index_name: str, key_columns: list, key_col_types: list, is_unique: bool = False):
        """
        为指定表创建B+树索引。
        :param table_name: 表名
        :param index_name: 索引名
        :param key_columns: 作为索引键的列名列表
        :param key_col_types: 索引键的类型列表
        :param is_unique: 是否唯一索引
        :return: BTreeManager实例
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import CreateIndexLogRecord
        log_record = CreateIndexLogRecord(transaction.id, table_name, index_name, key_columns, key_col_types, is_unique)
        self.log_manager.append(transaction, log_record)
        file_name = f"{table_name}_{index_name}.idx"
        self.catalog_manager.create_index(transaction, table_name, index_name, key_columns, file_name, key_col_types, index_type='BTREE', is_unique=is_unique)
        tm, bp = self._get_indexspace_and_buffer(table_name, index_name, key_col_types)
        table_info = self.catalog_manager.get_table(table_name)
        idx_info = table_info.indexes[index_name]
        if idx_info.root_page_id is None:
            from src.storage.btreepage import BTreeLeafPage
            new_page = bp.new_page(page_cls=BTreeLeafPage, key_col_types=key_col_types)
            idx_info.root_page_id = new_page.page_id
            self.catalog_manager.update_index_root_page(transaction, table_name, index_name, new_page.page_id)
            bp.flush_page(new_page.page_id)
        from src.engine.storage.btree_manager import BTreeManager
        bptm = BTreeManager(bp, self.catalog_manager, table_name, index_name, key_col_types)
        schema = table_info.columns
        key_indices = [i for i, col in enumerate(schema) if getattr(col, 'column_name', None) in key_columns]
        for row_id, row in self.scan(transaction, table_name):
            key = tuple(row[i] for i in key_indices)
            bptm.insert(transaction, key, row_id, 0)
        return bptm

    def drop_index(self, transaction, table_name: str, index_name: str):
        """
        删除指定表的索引。
        :param table_name: 表名
        :param index_name: 索引名
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import DropIndexLogRecord
        log_record = DropIndexLogRecord(transaction.id, table_name, index_name)
        self.log_manager.append(transaction, log_record)
        table_info = self.catalog_manager.get_table(table_name)
        if index_name not in table_info.indexes:
            raise ValueError(f"索引 {index_name} 不存在于表 {table_name}")
        idx_info = table_info.indexes[index_name]
        key = f"{table_name}::{index_name}"
        if hasattr(self, 'indexspace_managers') and key in self.indexspace_managers:
            tm = self.indexspace_managers[key]
            tm.close()
            del self.indexspace_managers[key]
        if hasattr(self, 'index_buffer_pools') and key in self.index_buffer_pools:
            del self.index_buffer_pools[key]
        file_path = os.path.join(self.data_dir, idx_info.file_name)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except PermissionError:
                pass
        del table_info.indexes[index_name]
        # self.catalog_manager._save_catalog()  # 事务化后由上层统一持久化

    def find_by_index(self,transaction, table_name: str, index_name: str, key: tuple):
        """
        通过B+树索引查找指定key对应的row_id。
        :param table_name: 表名
        :param index_name: 索引名
        :param key: 索引键（元组）
        :return: row_id 或 None
        """
        table_info = self.catalog_manager.get_table(table_name)
        idx_info = table_info.indexes.get(index_name)
        if idx_info is None:
            raise ValueError(f"索引 {index_name} 不存在于表 {table_name}")
        tm, bp = self._get_indexspace_and_buffer(table_name, index_name, idx_info.key_col_types)
        from src.engine.storage.btree_manager import BTreeManager
        bptm = BTreeManager(bp, self.catalog_manager, table_name, index_name, idx_info.key_col_types)
        
        return bptm.search(transaction, key)
    
    def get_row(self, transaction, table_name: str, row_id: Any) -> Optional[Tuple[Any, Tuple]]:
        
        print("get_row:",row_id)
        """根据 row_id 获取单条记录。"""
        table_info = self.catalog_manager.get_table(table_name)
        schema = table_info.columns
        serializer = TupleSerializer(schema)
        tm, bp = self._get_tablespace_and_buffer(table_name)
        heap_file = HeapFileManager(bp, table_info, self.catalog_manager)
        record_size = serializer.get_record_size()
        page_id, record_no = row_id
        # 加锁以保证读一致性
        self.lock_manager.acquire(transaction, LockMode.SHARED, ResourceID(table_name, page_id, record_no))
        row_bytes = heap_file.get_record(transaction, row_id, record_size)
        if row_bytes:
            row_tuple = serializer.deserialize(row_bytes)
            return (row_id, row_tuple)
        return None
    
    def get_page_for_recovery(self, resource_id: ResourceID) -> BasePage:
        """恢复专用：根据 ResourceID 获取页面对象"""
        # 检查表是否存在，如果不存在则跳过恢复操作
        if not self.table_exists(resource_id.table_name):
            logger.warning(f"[Recovery] 跳过不存在的表: {resource_id.table_name}")
            return None
        
        _, buffer_pool = self._get_tablespace_and_buffer(resource_id.table_name)
        # BufferPool 需要一个 get_page_without_pin 的方法，或者 pin_count=0
        return buffer_pool.get_page(resource_id.page_id)

    def table_exists(self, table_name: str) -> bool:
        """判断表是否存在。"""
        return self.catalog_manager.table_exists(table_name)

    def truncate_table(self, table_name: str):
        """清空表中所有数据（保留表结构）。"""
        # 1. 删除物理文件
        self.drop_table_file(table_name)
        # 2. 重新创建表空间和缓冲池
        table_info = self.catalog_manager.get_table(table_name)
        file_path = os.path.join(self.data_dir, table_info.file_name)
        tm = TablespaceManager(file_path)
        bp = BufferPool(tm, self.buffer_size)
        self.tablespace_managers[table_name] = tm
        self.buffer_pools[table_name] = bp
        # 3. 重置统计信息
        self.catalog_manager.update_table_stats(table_name, 0, 0)

    
    def _create_primary_key_index(self, transaction, table_name: str):
        """为表的主键自动创建唯一索引"""
        table_info = self.catalog_manager.get_table(table_name)
        primary_key_columns = []
        
        # 查找主键列
        for col in table_info.columns:
            if getattr(col, 'is_primary_key', False):
                primary_key_columns.append(col.column_name)
        
        # 如果找到主键列，创建唯一索引
        if primary_key_columns:
            primary_key_col = primary_key_columns[0]  # 假设单列主键
            index_name = f"pk_{table_name}_{primary_key_col}"
            
            # 获取主键列的数据类型
            col_type = None
            for col in table_info.columns:
                if col.column_name == primary_key_col:
                    col_type = col.data_type
                    break
            
            if col_type:
                # 创建主键索引
                key_col_types = [self._get_type_code(col_type)]
                try:
                    self.create_index(
                        transaction, 
                        table_name, 
                        index_name, 
                        [primary_key_col], 
                        key_col_types, 
                        is_unique=True
                    )
                    logger.info(f"已为主键列 '{primary_key_col}' 创建唯一索引 '{index_name}'")
                except Exception as e:
                    logger.error(f"创建主键索引失败: {e}")
    
    def _check_primary_key_uniqueness(self, transaction, table_name: str, column_name: str, 
                                     value: Any, exclude_row_id: Any = None) -> Optional[Any]:
        """检查主键唯一性"""
        table_info = self.catalog_manager.get_table(table_name)
        
        # 查找主键索引
        primary_key_index = None
        for index_name, index_info in table_info.indexes.items():
            if (index_info.is_unique and 
                len(index_info.column_names) == 1 and 
                index_info.column_names[0] == column_name):
                primary_key_index = index_name
                break
        
        if primary_key_index:
            # 通过索引查找
            try:
                existing_row_id = self.find_by_index(table_name, primary_key_index, (value,))
                if existing_row_id and existing_row_id != exclude_row_id:
                    return existing_row_id
            except Exception:
                # 索引查找失败，回退到全表扫描
                pass
        
        # 回退到全表扫描检查唯一性
        try:
            for row_id, row in self.scan(transaction, table_name):
                if row_id == exclude_row_id:
                    continue
                
                # 获取主键列的值
                for i, col in enumerate(table_info.columns):
                    if col.column_name == column_name:
                        if row[i] == value:
                            return row_id
                        break
        except Exception:
            pass
        
        return None
    
    def _get_type_code(self, data_type: str) -> int:
        """将数据类型转换为内部类型代码"""
        type_mapping = {
            'INT': 1,
            'INTEGER': 1,
            'VARCHAR': 2,
            'DECIMAL': 3,
            'DATE': 4,
            'TIMESTAMP': 5,
            'BOOLEAN': 6,
            'TEXT': 2
        }
        return type_mapping.get(data_type.upper(), 1)  # 默认为INT


    def create_view(self, transaction, view_name: str, definition: str, schema_name: str = 'public', creator: str = 'system', is_updatable: bool = False):
        """
        事务化创建视图，自动记录日志。
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import CreateViewLogRecord
        log_record = CreateViewLogRecord(transaction.id, view_name, definition, is_updatable)
        self.log_manager.append(transaction, log_record)
        self.catalog_manager.create_view(
            view_name=view_name,
            definition=definition,
            schema_name=schema_name,
            creator=creator,
            is_updatable=is_updatable,
            transaction=transaction
        )

    def delete_view(self, transaction, view_name: str):
        """
        事务化删除视图，自动记录日志。
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import DropViewLogRecord
        log_record = DropViewLogRecord(transaction.id, view_name)
        self.log_manager.append(transaction, log_record)
        self.catalog_manager.delete_view(view_name,transaction)

    def alter_view(self, transaction, view_name: str, definition: str, is_updatable: bool = None):
        """
        事务化修改视图，自动记录日志。
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import AlterViewLogRecord
        # 获取当前is_updatable状态，如果未指定则保持原值
        if is_updatable is None:
            is_updatable = self.catalog_manager.get_view(view_name).is_updatable
        log_record = AlterViewLogRecord(transaction.id, view_name, definition, is_updatable)
        self.log_manager.append(transaction, log_record)
        self.catalog_manager.update_view(view_name, definition, is_updatable,transaction)

    def create_trigger(self, transaction, trigger_name: str, table_name: str, timing: str, events: list, is_row_level: bool, when_condition: str, trigger_body: list):
        """
        事务化创建触发器，自动记录日志。
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import CreateTriggerLogRecord
        log_record = CreateTriggerLogRecord(transaction.id, trigger_name, table_name, timing, events, is_row_level, when_condition, trigger_body)
        self.log_manager.append(transaction, log_record)
        self.catalog_manager.create_trigger(trigger_name, table_name, timing, events, is_row_level, when_condition, trigger_body,transaction)

    def alter_trigger(self, transaction, trigger_name: str, table_name: str, timing: str, events: list, is_row_level: bool, when_condition: str, trigger_body: list):
        """
        事务化修改触发器，自动记录日志。
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import AlterTriggerLogRecord
        log_record = AlterTriggerLogRecord(transaction.id, trigger_name, table_name, timing, events, is_row_level, when_condition, trigger_body)
        self.log_manager.append(transaction, log_record)
        # 这里直接覆盖原有触发器
        self.catalog_manager.update_trigger(trigger_name, table_name, timing, events, is_row_level, when_condition, trigger_body,transaction)

    def delete_trigger(self, transaction, trigger_name: str):
        """
        事务化删除触发器，自动记录日志。
        """
        self.lock_manager.acquire(transaction, LockMode.EXCLUSIVE, ResourceID('__catalog__'))
        from src.engine.transaction.log_manager import DropTriggerLogRecord
        log_record = DropTriggerLogRecord(transaction.id, trigger_name)
        self.log_manager.append(transaction, log_record)
        self.catalog_manager.delete_trigger(trigger_name,transaction)

