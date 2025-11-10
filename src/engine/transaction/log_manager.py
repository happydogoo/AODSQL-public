# log_manager.py

import threading
import struct
import json
from enum import IntEnum # 使用 IntEnum 方便与字节直接转换
from typing import Dict, BinaryIO, Type, Optional, Generator, TYPE_CHECKING
from loguru import logger

from .transaction import Transaction, IsolationLevel
from .lock_manager import ResourceID
from src.engine.catalog_manager import TableInfo, ColumnInfo
from src.engine.storage.index_manager import IndexManager
from src.engine.storage.tuple_serializer import TupleSerializer

# from engine.real_storage_engine import RealStorageEngine
# --- 3. 添加这个代码块 ---
if TYPE_CHECKING:
    from src.engine.storage.real_storage_engine import RealStorageEngine
# --- 常量定义 ---
NULL_LSN = 0
PAGE_SIZE = 4096 # 假设页大小为 4KB

# --- 常量和日志类型定义 (与上一版相同) ---
class LogType(IntEnum):
    UPDATE = 1; INSERT = 2; DELETE = 3
    COMMIT = 10; ABORT = 11; CLR = 12
    BEGIN_CHECKPOINT = 20; END_CHECKPOINT = 21
    CREATE_TABLE = 30; DROP_TABLE = 31
    CREATE_INDEX = 32; DROP_INDEX = 33
    CREATE_VIEW = 34; DROP_VIEW = 35; ALTER_VIEW = 36
    CREATE_TRIGGER = 37; DROP_TRIGGER = 38; ALTER_TRIGGER = 39

# --- 日志记录的继承体系 ---

class LogRecord:
    """日志记录的抽象基类"""
    # 通用日志头格式: < LSN(Q), Prev_LSN(Q), Txn_ID(I), Type(B)
    HEADER_FORMAT = '<QQIB'
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    
    def __init__(self, transaction_id: int, log_type: LogType):
        self.lsn: int = NULL_LSN
        self.transaction_id: int = transaction_id
        self.prev_lsn: int = NULL_LSN
        self.log_type: LogType = log_type

    def to_bytes_with_prefix(self) -> bytes:
        """序列化日志记录，并加上长度前缀"""
        payload = self.to_bytes()
        # < Length(I) >
        return struct.pack('<I', len(payload)) + payload
    
    def to_bytes(self) -> bytes:
        """序列化日志记录为字节流 (仅头部)"""
        return struct.pack(self.HEADER_FORMAT, self.lsn, self.prev_lsn, self.transaction_id, self.log_type.value)
    
    def undo(self, storage_engine: 'RealStorageEngine'):
        """物理撤销操作。仅可撤销的日志类型需要实现。"""
        raise NotImplementedError

    def redo(self, storage_engine: 'RealStorageEngine'):
        """物理重做操作。仅数据修改类日志需要实现。"""
        raise NotImplementedError
    @staticmethod
    def from_bytes(data: bytes) -> Optional['LogRecord']:
        """
        工厂方法：从字节流反序列化为具体的 LogRecord 对象。
        这是整个日志读取的核心。
        """
        if len(data) < LogRecord.HEADER_SIZE:
            return None
            
        _, _, _, type_val = struct.unpack(LogRecord.HEADER_FORMAT, data[:LogRecord.HEADER_SIZE])
        log_type = LogType(type_val)
        
        # 根据日志类型，调用相应的子类进行反序列化
        log_class_map: Dict[LogType, Type[LogRecord]] = {
            LogType.UPDATE: UpdateLogRecord,
            LogType.INSERT: InsertLogRecord,
            LogType.DELETE: DeleteLogRecord,
            LogType.COMMIT: CommitLogRecord,
            LogType.ABORT: AbortLogRecord,
            LogType.CLR: CompensationLogRecord,
            LogType.BEGIN_CHECKPOINT: BeginCheckpointLogRecord,
            LogType.END_CHECKPOINT: EndCheckpointLogRecord,
            LogType.CREATE_TABLE: CreateTableLogRecord,
            LogType.DROP_TABLE: DropTableLogRecord,
            LogType.CREATE_INDEX: CreateIndexLogRecord,
            LogType.DROP_INDEX: DropIndexLogRecord,
            LogType.CREATE_VIEW: CreateViewLogRecord,
            LogType.DROP_VIEW: DropViewLogRecord,
            LogType.ALTER_VIEW: AlterViewLogRecord,
            LogType.CREATE_TRIGGER: CreateTriggerLogRecord,
            LogType.DROP_TRIGGER: DropTriggerLogRecord,
            LogType.ALTER_TRIGGER: AlterTriggerLogRecord,
        }
        
        if log_type in log_class_map:
            return log_class_map[log_type]._from_payload(data)
        
        raise ValueError(f"未知的日志类型: {log_type}")

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        """由子类实现的、从包含头部的完整字节流中解析的内部方法"""
        # 基类只解析头部
        lsn, prev_lsn, txn_id, type_val = struct.unpack(cls.HEADER_FORMAT, data[:cls.HEADER_SIZE])
        record = cls(txn_id)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class InsertLogRecord(LogRecord):
    PAYLOAD_FORMAT_PREFIX = '<HIIH'  # table_name_len, page_id, record_id, data_len
    PAYLOAD_PREFIX_SIZE = struct.calcsize(PAYLOAD_FORMAT_PREFIX)

    def __init__(self, transaction_id: int, resource_id: ResourceID, inserted_data: bytes):
        super().__init__(transaction_id, LogType.INSERT)
        self.resource_id = resource_id
        self.inserted_data = inserted_data

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        table_name_bytes = self.resource_id.table_name.encode('utf-8')
        payload = struct.pack(
            self.PAYLOAD_FORMAT_PREFIX,
            len(table_name_bytes),
            self.resource_id.page_id,
            self.resource_id.record_id,
            len(self.inserted_data)
        ) + table_name_bytes + self.inserted_data
        return header + payload

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        p_size = cls.PAYLOAD_PREFIX_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        tbl_len, page_id, rec_id, d_len = struct.unpack(
            cls.PAYLOAD_FORMAT_PREFIX, data[header_size : header_size + p_size]
        )
        offset = header_size + p_size
        tbl_name = data[offset : offset + tbl_len].decode('utf-8')
        offset += tbl_len
        inserted_data = data[offset : offset + d_len]
        record = cls(txn_id, ResourceID(tbl_name, page_id, rec_id), inserted_data)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

    def undo(self, storage_engine: 'RealStorageEngine', transaction: 'Transaction', clr_lsn: int):
        """
        撤销插入操作：即物理删除该记录。
        """

        # --- 1. 准备工作 ---
        table_info = storage_engine.catalog_manager.get_table(self.resource_id.table_name)
        serializer = TupleSerializer(table_info.columns)
        row_tuple = serializer.deserialize(self.inserted_data)

        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = storage_engine._get_indexspace_and_buffer(self.resource_id.table_name, index_name)
            index_buffer_pools[index_name] = idx_bp

        main_bp = storage_engine.buffer_pools.get(self.resource_id.table_name)
        index_manager = IndexManager(table_info, main_bp, index_buffer_pools, storage_engine.catalog_manager)
        row_id = (self.resource_id.page_id, self.resource_id.record_id)
        lsn_map = {index_name: clr_lsn for index_name in table_info.indexes}

        # --- 2. 删除索引条目 ---
        index_manager.delete_entry(transaction, row_tuple, row_id, lsn_map)
        
        # --- 3. 插入数据页记录 ---
        page = storage_engine.get_page_for_recovery(self.resource_id)
        buffer_pool = storage_engine.buffer_pools[self.resource_id.table_name]
        try:
            page.mark_as_deleted(transaction, self.resource_id.record_id, len(self.inserted_data), clr_lsn)
        finally:
            buffer_pool.unpin_page(self.resource_id.page_id, is_dirty=True)

    def redo(self, storage_engine: 'RealStorageEngine', transaction: 'Transaction', **kwargs):
        
        # 恢复索引
        table_info = storage_engine.catalog_manager.get_table(self.resource_id.table_name)
        serializer = TupleSerializer(table_info.columns)
        row_tuple = serializer.deserialize(self.inserted_data)
        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = storage_engine._get_indexspace_and_buffer(self.resource_id.table_name, index_name)
            index_buffer_pools[index_name] = idx_bp
        main_bp = storage_engine.buffer_pools.get(self.resource_id.table_name)
        index_manager = IndexManager(table_info, main_bp, index_buffer_pools, storage_engine.catalog_manager)
        row_id = (self.resource_id.page_id, self.resource_id.record_id)
        lsn_map = {index_name: self.lsn for index_name in table_info.indexes}

        # --- 2. 插入索引条目 ---
        index_manager.insert_entry(transaction, row_tuple, row_id, lsn_map)
        
        page = storage_engine.get_page_for_recovery(self.resource_id)
        buffer_pool = storage_engine.buffer_pools[self.resource_id.table_name]
        try:
            # 关键：插入到指定 record_id
            logger.debug(f"Inserting record at record_id: {self.resource_id.record_id}，{self.inserted_data}")

            page.insert_record_at(transaction, self.resource_id.record_id, self.inserted_data, self.lsn)
        finally:
            buffer_pool.unpin_page(self.resource_id.page_id, is_dirty=True)

    def get_undo_info(self):
        return (self.resource_id, len(self.inserted_data))

class DeleteLogRecord(LogRecord):
    PAYLOAD_FORMAT_PREFIX = '<HIIH'  # table_name_len, page_id, record_id, data_len
    PAYLOAD_PREFIX_SIZE = struct.calcsize(PAYLOAD_FORMAT_PREFIX)

    def __init__(self, transaction_id: int, resource_id: ResourceID, deleted_data: bytes):
        super().__init__(transaction_id, LogType.DELETE)
        self.resource_id = resource_id
        self.deleted_data = deleted_data

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        table_name_bytes = self.resource_id.table_name.encode('utf-8')
        payload = struct.pack(
            self.PAYLOAD_FORMAT_PREFIX,
            len(table_name_bytes),
            self.resource_id.page_id,
            self.resource_id.record_id,
            len(self.deleted_data)
        ) + table_name_bytes + self.deleted_data
        return header + payload

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        p_size = cls.PAYLOAD_PREFIX_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        tbl_len, page_id, rec_id, d_len = struct.unpack(
            cls.PAYLOAD_FORMAT_PREFIX, data[header_size : header_size + p_size]
        )
        offset = header_size + p_size
        tbl_name = data[offset : offset + tbl_len].decode('utf-8')
        offset += tbl_len
        deleted_data = data[offset : offset + d_len]
        record = cls(txn_id, ResourceID(tbl_name, page_id, rec_id), deleted_data)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

    def undo(self, storage_engine: 'RealStorageEngine', transaction: 'Transaction', clr_lsn: int):
        """
        撤销删除操作：将数据插回。
        """
        # --- 1. 准备工作 ---
        table_info = storage_engine.catalog_manager.get_table(self.resource_id.table_name)
        serializer = TupleSerializer(table_info.columns)
        row_tuple = serializer.deserialize(self.deleted_data)

        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = storage_engine._get_indexspace_and_buffer(self.resource_id.table_name, index_name)
            index_buffer_pools[index_name] = idx_bp

        main_bp = storage_engine.buffer_pools.get(self.resource_id.table_name)
        index_manager = IndexManager(table_info, main_bp, index_buffer_pools, storage_engine.catalog_manager)
        row_id = (self.resource_id.page_id, self.resource_id.record_id)
        lsn_map = {index_name: clr_lsn for index_name in table_info.indexes}

        # --- 2. 插入索引条目 ---
        index_manager.insert_entry(transaction, row_tuple, row_id, lsn_map)

        # --- 3. 插入数据页记录 ---
        page = storage_engine.get_page_for_recovery(self.resource_id)
        buffer_pool = storage_engine.buffer_pools[self.resource_id.table_name]
        try:
            page.insert_record(transaction, self.deleted_data, clr_lsn)
        finally:
            buffer_pool.unpin_page(self.resource_id.page_id, is_dirty=True)

    def redo(self, storage_engine: 'RealStorageEngine', transaction: 'Transaction', **kwargs):
        # --- 1. 准备工作 ---
        table_info = storage_engine.catalog_manager.get_table(self.resource_id.table_name)
        serializer = TupleSerializer(table_info.columns)
        row_tuple = serializer.deserialize(self.deleted_data)

        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = storage_engine._get_indexspace_and_buffer(self.resource_id.table_name, index_name)
            index_buffer_pools[index_name] = idx_bp

        main_bp = storage_engine.buffer_pools.get(self.resource_id.table_name)
        index_manager = IndexManager(table_info, main_bp, index_buffer_pools, storage_engine.catalog_manager)
        row_id = (self.resource_id.page_id, self.resource_id.record_id)
        lsn_map = {index_name: self.lsn for index_name in table_info.indexes}

        # --- 2. 删除索引条目 ---
        index_manager.delete_entry(transaction, row_tuple, row_id, lsn_map)

        # --- 3. 删除数据页记录 ---        
        page = storage_engine.get_page_for_recovery(self.resource_id)
        buffer_pool = storage_engine.buffer_pools[self.resource_id.table_name]
        try:
            page.mark_as_deleted(transaction, self.resource_id.record_id, len(self.deleted_data), self.lsn)
        finally:
            buffer_pool.unpin_page(self.resource_id.page_id, is_dirty=True)

    def get_undo_info(self):
        return (self.resource_id, self.deleted_data)

class UpdateLogRecord(LogRecord):
    """更新操作的日志记录"""
    # 负载格式: < TableNameLen(H), PageID(I), RecordID(I), BeforeImageLen(H), AfterImageLen(H) >
    PAYLOAD_FORMAT_PREFIX = '<HIIHH'
    PAYLOAD_PREFIX_SIZE = struct.calcsize(PAYLOAD_FORMAT_PREFIX)

    def __init__(self, transaction_id: int, resource_id: ResourceID, before_image: bytes, after_image: bytes):
        super().__init__(transaction_id, LogType.UPDATE)
        self.resource_id = resource_id
        self.before_image = before_image
        self.after_image = after_image

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        table_name_bytes = self.resource_id.table_name.encode('utf-8')
        payload = struct.pack(
            self.PAYLOAD_FORMAT_PREFIX,
            len(table_name_bytes),
            self.resource_id.page_id,
            self.resource_id.record_id,
            len(self.before_image),
            len(self.after_image)
        ) + table_name_bytes + self.before_image + self.after_image
        return header + payload

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        p_size = cls.PAYLOAD_PREFIX_SIZE
        
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        
        tbl_len, page_id, rec_id, b_len, a_len = struct.unpack(
            cls.PAYLOAD_FORMAT_PREFIX, data[header_size : header_size + p_size]
        )
        
        offset = header_size + p_size
        tbl_name = data[offset : offset + tbl_len].decode('utf-8')
        offset += tbl_len
        before_image = data[offset : offset + b_len]
        offset += b_len
        after_image = data[offset : offset + a_len]

        record = cls(txn_id, ResourceID(tbl_name, page_id, rec_id), before_image, after_image)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record
    
    def undo(self, storage_engine: 'RealStorageEngine', transaction: 'Transaction', clr_lsn: int):
        """撤销更新，即将 before_image 写回"""
        # --- 1. 准备工作 ---
        table_info = storage_engine.catalog_manager.get_table(self.resource_id.table_name)
        serializer = TupleSerializer(table_info.columns)
        old_row = serializer.deserialize(self.before_image)
        new_row = serializer.deserialize(self.after_image)
        
        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = storage_engine._get_indexspace_and_buffer(self.resource_id.table_name, index_name)
            index_buffer_pools[index_name] = idx_bp

        main_bp = storage_engine.buffer_pools.get(self.resource_id.table_name)
        index_manager = IndexManager(table_info, main_bp, index_buffer_pools, storage_engine.catalog_manager)
        row_id = (self.resource_id.page_id, self.resource_id.record_id)
        lsn_map = {index_name: clr_lsn for index_name in table_info.indexes} # Undo uses CLR's LSN

        # --- 2. 回滚索引 (从 new_row 更新回 old_row) ---
        index_manager.update_entries(transaction, new_row, old_row, row_id, lsn_map)

        # --- 3. 回滚数据页 ---        
        page = storage_engine.get_page_for_recovery(self.resource_id)
        buffer_pool = storage_engine.buffer_pools[self.resource_id.table_name]
        try:

            page.update_record(transaction, self.resource_id.record_id, self.before_image, clr_lsn)

        finally:
            buffer_pool.unpin_page(self.resource_id.page_id, is_dirty=True)

    def redo(self, storage_engine: 'RealStorageEngine', transaction: 'Transaction', **kwargs):
        
        # --- 1. 准备工作：反序列化数据并准备 IndexManager ---
        table_info = storage_engine.catalog_manager.get_table(self.resource_id.table_name)
        serializer = TupleSerializer(table_info.columns)
        old_row = serializer.deserialize(self.before_image)
        new_row = serializer.deserialize(self.after_image)
        
        index_buffer_pools = {}
        for index_name in table_info.indexes:
            _, idx_bp = storage_engine._get_indexspace_and_buffer(self.resource_id.table_name, index_name)
            index_buffer_pools[index_name] = idx_bp
        
        # 注意：这里的 bp 参数对于 update_entries 来说不是必需的，可以传 None 或主表的 bp
        main_bp = storage_engine.buffer_pools.get(self.resource_id.table_name)
        index_manager = IndexManager(table_info, main_bp, index_buffer_pools, storage_engine.catalog_manager)
        row_id = (self.resource_id.page_id, self.resource_id.record_id)
        lsn_map = {index_name: self.lsn for index_name in table_info.indexes}

        # --- 2. 更新索引 ---
        index_manager.update_entries(transaction, old_row, new_row, row_id, lsn_map)
        
        # --- 3. 更新数据页 (原有逻辑) ---
        page = storage_engine.get_page_for_recovery(self.resource_id)
        buffer_pool = storage_engine.buffer_pools[self.resource_id.table_name]
        try:
            logger.debug(f"更新重做：{self.lsn}")
            page.update_record(transaction, self.resource_id.record_id, self.after_image, self.lsn)
        finally:
            buffer_pool.unpin_page(self.resource_id.page_id, is_dirty=True)

    def get_undo_info(self):
        return (self.resource_id, self.before_image)

# 标记事务提交
class CommitLogRecord(LogRecord):
    def __init__(self, transaction_id: int):
        super().__init__(transaction_id, LogType.COMMIT)
    
    # 没有额外负载，只需实现 _from_payload
    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        return super()._from_payload(data)
# 标记事务中止
class AbortLogRecord(LogRecord):
    def __init__(self, transaction_id: int):
        super().__init__(transaction_id, LogType.ABORT)

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        return super()._from_payload(data)
# 记录一次 undo 操作。
class CompensationLogRecord(LogRecord):
    """补偿日志记录 (CLR), 自包含 undo 操作所需的所有信息"""
    PAYLOAD_FORMAT_PREFIX = '<QB'  # undo_next_lsn(Q), original_log_type(B)

    def __init__(self, transaction_id: int, undo_next_lsn: int, original_log_type: LogType, undo_info: tuple):
        super().__init__(transaction_id, LogType.CLR)
        self.undo_next_lsn = undo_next_lsn
        self.original_log_type = original_log_type
        self.undo_info = undo_info  # (resource_id, undo_data)
        self.resource_id = undo_info[0]

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        prefix = struct.pack(self.PAYLOAD_FORMAT_PREFIX, self.undo_next_lsn, self.original_log_type.value)
        resource_id, undo_data = self.undo_info
        table_name_bytes = resource_id.table_name.encode('utf-8')
        info_header = struct.pack('<H', len(table_name_bytes)) + table_name_bytes
        info_middle = struct.pack('<II', resource_id.page_id, resource_id.record_id)
        if self.original_log_type in {LogType.UPDATE, LogType.DELETE}:
            info_data = struct.pack(f'<H{len(undo_data)}s', len(undo_data), undo_data)
        elif self.original_log_type == LogType.INSERT:
            info_data = struct.pack('<I', undo_data)
        else:
            raise ValueError('Unsupported CLR original_log_type')
        return header + prefix + info_header + info_middle + info_data

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        prefix_size = struct.calcsize(cls.PAYLOAD_FORMAT_PREFIX)
        offset = header_size
        undo_next_lsn, orig_type_val = struct.unpack(cls.PAYLOAD_FORMAT_PREFIX, data[offset : offset + prefix_size])
        original_log_type = LogType(orig_type_val)
        offset += prefix_size
        tbl_len, = struct.unpack('<H', data[offset : offset + 2])
        offset += 2
        tbl_name = data[offset : offset + tbl_len].decode('utf-8')
        offset += tbl_len
        page_id, rec_id = struct.unpack('<II', data[offset : offset + 8])
        offset += 8
        resource_id = ResourceID(tbl_name, page_id, rec_id)
        if original_log_type in {LogType.UPDATE, LogType.DELETE}:
            data_len, = struct.unpack('<H', data[offset : offset + 2])
            offset += 2
            undo_data = data[offset : offset + data_len]
        elif original_log_type == LogType.INSERT:
            undo_data, = struct.unpack('<I', data[offset : offset + 4])
        else:
            raise ValueError('Unsupported CLR original_log_type from payload')
        undo_info = (resource_id, undo_data)
        record = cls(txn_id, undo_next_lsn, original_log_type, undo_info)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

    def redo(self, storage_engine: 'RealStorageEngine', transaction: 'Transaction', **kwargs):
        resource_id, undo_data = self.undo_info
        page = storage_engine.get_page_for_recovery(resource_id)
        
        # 如果表不存在，跳过恢复操作
        if page is None:
            print(f"[Recovery] 跳过不存在的表的恢复操作: {resource_id.table_name}")
            return
        
        # 检查buffer_pool是否存在
        if resource_id.table_name not in storage_engine.buffer_pools:
            print(f"[Recovery] 跳过不存在的表的buffer_pool: {resource_id.table_name}")
            return
            
        buffer_pool = storage_engine.buffer_pools[resource_id.table_name]
        try:
            if self.original_log_type == LogType.UPDATE:
                page.update_record(transaction, resource_id.record_id, undo_data, self.lsn)
            elif self.original_log_type == LogType.INSERT:
                page.mark_as_deleted(transaction, resource_id.record_id, undo_data, self.lsn)
            elif self.original_log_type == LogType.DELETE:
                page.insert_record(transaction, undo_data, self.lsn)
            else:
                raise ValueError('Unsupported CLR original_log_type')
        finally:
            buffer_pool.unpin_page(resource_id.page_id, is_dirty=True)

# 标记检查点过程的开始。
class BeginCheckpointLogRecord(LogRecord):
    def __init__(self, transaction_id: int = -1): # Checkpoint 不是事务的一部分
        super().__init__(transaction_id, LogType.BEGIN_CHECKPOINT)
# 标记检查点过程的结束。
class EndCheckpointLogRecord(LogRecord):
    def __init__(self, att: Dict[int, int], dpt: Dict[int, int], transaction_id: int = -1):
        super().__init__(transaction_id, LogType.END_CHECKPOINT)
        self.att = att # 活跃事务表: {txn_id: last_lsn}
        self.dpt = dpt # 脏页表: {page_id: recovery_lsn}

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        payload = json.dumps({'att': self.att, 'dpt': self.dpt}).encode('utf-8')
        return header + payload
        
    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        payload = json.loads(data[header_size:].decode('utf-8'))
        record = cls(payload['att'], payload['dpt'], txn_id)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class CreateTableLogRecord(LogRecord):
    def __init__(self, transaction_id: int, table_name: str, schema: list):
        super().__init__(transaction_id, LogType.CREATE_TABLE)
        self.table_name = table_name
        self.schema = schema  # [(col, type), ...]
    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        table_name_bytes = self.table_name.encode('utf-8')
        schema_bytes = json.dumps(self.schema).encode('utf-8')
        return header + struct.pack('<H', len(table_name_bytes)) + table_name_bytes + struct.pack('<I', len(schema_bytes)) + schema_bytes
    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        tbl_len = struct.unpack('<H', data[header_size:header_size+2])[0]
        offset = header_size + 2
        table_name = data[offset:offset+tbl_len].decode('utf-8')
        offset += tbl_len
        schema_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        schema = json.loads(data[offset:offset+schema_len].decode('utf-8'))
        record = cls(txn_id, table_name, schema)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class DropTableLogRecord(LogRecord):
    def __init__(self, transaction_id: int, table_name: str):
        super().__init__(transaction_id, LogType.DROP_TABLE)
        self.table_name = table_name
    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        table_name_bytes = self.table_name.encode('utf-8')
        return header + struct.pack('<H', len(table_name_bytes)) + table_name_bytes
    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        tbl_len = struct.unpack('<H', data[header_size:header_size+2])[0]
        table_name = data[header_size+2:header_size+2+tbl_len].decode('utf-8')
        record = cls(txn_id, table_name)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class CreateIndexLogRecord(LogRecord):
    def __init__(self, transaction_id: int, table_name: str, index_name: str, columns: list, key_col_types: list, is_unique: bool):
        super().__init__(transaction_id, LogType.CREATE_INDEX)
        self.table_name = table_name
        self.index_name = index_name
        self.columns = columns
        self.key_col_types = key_col_types
        self.is_unique = is_unique
    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        table_name_bytes = self.table_name.encode('utf-8')
        index_name_bytes = self.index_name.encode('utf-8')
        payload_dict = {
            'cols': self.columns,
            'types': self.key_col_types,
            'unique': self.is_unique
        }
        payload_json_bytes = json.dumps(payload_dict).encode('utf-8')
        return (header +
                struct.pack('<H', len(table_name_bytes)) + table_name_bytes +
                struct.pack('<H', len(index_name_bytes)) + index_name_bytes +
                struct.pack('<I', len(payload_json_bytes)) + payload_json_bytes)
    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        offset = header_size
        tbl_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        table_name = data[offset:offset+tbl_len].decode('utf-8')
        offset += tbl_len
        idx_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        index_name = data[offset:offset+idx_len].decode('utf-8')
        offset += idx_len
        json_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        payload_dict = json.loads(data[offset:offset+json_len].decode('utf-8'))
        record = cls(txn_id, table_name, index_name, payload_dict['cols'], payload_dict['types'], payload_dict['unique'])
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class DropIndexLogRecord(LogRecord):
    def __init__(self, transaction_id: int, table_name: str, index_name: str):
        super().__init__(transaction_id, LogType.DROP_INDEX)
        self.table_name = table_name
        self.index_name = index_name
    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        table_name_bytes = self.table_name.encode('utf-8')
        index_name_bytes = self.index_name.encode('utf-8')
        return (header + struct.pack('<H', len(table_name_bytes)) + table_name_bytes +
                struct.pack('<H', len(index_name_bytes)) + index_name_bytes)
    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        tbl_len = struct.unpack('<H', data[header_size:header_size+2])[0]
        offset = header_size + 2
        table_name = data[offset:offset+tbl_len].decode('utf-8')
        offset += tbl_len
        idx_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        index_name = data[offset:offset+idx_len].decode('utf-8')
        record = cls(txn_id, table_name, index_name)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

# --- 新增视图DDL日志记录类型 ---
class CreateViewLogRecord(LogRecord):
    def __init__(self, transaction_id: int, view_name: str, definition: str, is_updatable: bool):
        super().__init__(transaction_id, LogType.CREATE_VIEW)
        self.view_name = view_name
        self.definition = definition
        self.is_updatable = is_updatable

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        view_name_bytes = self.view_name.encode('utf-8')
        definition_bytes = self.definition.encode('utf-8')
        payload_prefix = struct.pack('<HIB', len(view_name_bytes), len(definition_bytes), int(self.is_updatable))
        return header + payload_prefix + view_name_bytes + definition_bytes

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        offset = header_size
        v_len, d_len, is_updatable_val = struct.unpack('<HIB', data[offset : offset + 7])
        offset += 7
        view_name = data[offset : offset + v_len].decode('utf-8')
        offset += v_len
        definition = data[offset : offset + d_len].decode('utf-8')
        record = cls(txn_id, view_name, definition, bool(is_updatable_val))
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class DropViewLogRecord(LogRecord):
    def __init__(self, transaction_id: int, view_name: str):
        super().__init__(transaction_id, LogType.DROP_VIEW)
        self.view_name = view_name

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        view_name_bytes = self.view_name.encode('utf-8')
        return header + struct.pack('<H', len(view_name_bytes)) + view_name_bytes

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        v_len = struct.unpack('<H', data[header_size : header_size + 2])[0]
        view_name = data[header_size + 2 : header_size + 2 + v_len].decode('utf-8')
        record = cls(txn_id, view_name)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class AlterViewLogRecord(LogRecord):
    def __init__(self, transaction_id: int, view_name: str, definition: str, is_updatable: bool):
        super().__init__(transaction_id, LogType.ALTER_VIEW)
        self.view_name = view_name
        self.definition = definition
        self.is_updatable = is_updatable

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        view_name_bytes = self.view_name.encode('utf-8')
        definition_bytes = self.definition.encode('utf-8')
        payload_prefix = struct.pack('<HIB', len(view_name_bytes), len(definition_bytes), int(self.is_updatable))
        return header + payload_prefix + view_name_bytes + definition_bytes

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        offset = header_size
        v_len, d_len, is_updatable_val = struct.unpack('<HIB', data[offset : offset + 7])
        offset += 7
        view_name = data[offset : offset + v_len].decode('utf-8')
        offset += v_len
        definition = data[offset : offset + d_len].decode('utf-8')
        record = cls(txn_id, view_name, definition, bool(is_updatable_val))
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

# --- 新增触发器DDL日志记录类型 ---
class CreateTriggerLogRecord(LogRecord):
    def __init__(self, transaction_id: int, trigger_name: str, table_name: str, timing: str, events: list, is_row_level: bool, when_condition: str, trigger_body: list):
        super().__init__(transaction_id, LogType.CREATE_TRIGGER)
        self.trigger_name = trigger_name
        self.table_name = table_name
        self.timing = timing
        self.events = events
        self.is_row_level = is_row_level
        self.when_condition = when_condition
        self.trigger_body = trigger_body

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        trigger_name_bytes = self.trigger_name.encode('utf-8')
        table_name_bytes = self.table_name.encode('utf-8')
        timing_bytes = self.timing.encode('utf-8')
        events_json = json.dumps(self.events).encode('utf-8')
        when_bytes = (self.when_condition or '').encode('utf-8')
        body_json = json.dumps(self.trigger_body).encode('utf-8')
        payload = (
            struct.pack('<H', len(trigger_name_bytes)) + trigger_name_bytes +
            struct.pack('<H', len(table_name_bytes)) + table_name_bytes +
            struct.pack('<H', len(timing_bytes)) + timing_bytes +
            struct.pack('<I', len(events_json)) + events_json +
            struct.pack('<?', self.is_row_level) +
            struct.pack('<H', len(when_bytes)) + when_bytes +
            struct.pack('<I', len(body_json)) + body_json
        )
        return header + payload

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        offset = header_size
        tname_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        trigger_name = data[offset:offset+tname_len].decode('utf-8')
        offset += tname_len
        tbl_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        table_name = data[offset:offset+tbl_len].decode('utf-8')
        offset += tbl_len
        timing_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        timing = data[offset:offset+timing_len].decode('utf-8')
        offset += timing_len
        events_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        events = json.loads(data[offset:offset+events_len].decode('utf-8'))
        offset += events_len
        is_row_level = struct.unpack('<?', data[offset:offset+1])[0]
        offset += 1
        when_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        when_condition = data[offset:offset+when_len].decode('utf-8')
        offset += when_len
        body_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        trigger_body = json.loads(data[offset:offset+body_len].decode('utf-8'))
        record = cls(txn_id, trigger_name, table_name, timing, events, is_row_level, when_condition, trigger_body)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class DropTriggerLogRecord(LogRecord):
    def __init__(self, transaction_id: int, trigger_name: str):
        super().__init__(transaction_id, LogType.DROP_TRIGGER)
        self.trigger_name = trigger_name

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        trigger_name_bytes = self.trigger_name.encode('utf-8')
        return header + struct.pack('<H', len(trigger_name_bytes)) + trigger_name_bytes

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        tname_len = struct.unpack('<H', data[header_size:header_size+2])[0]
        trigger_name = data[header_size+2:header_size+2+tname_len].decode('utf-8')
        record = cls(txn_id, trigger_name)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class AlterTriggerLogRecord(LogRecord):
    def __init__(self, transaction_id: int, trigger_name: str, table_name: str, timing: str, events: list, is_row_level: bool, when_condition: str, trigger_body: list):
        super().__init__(transaction_id, LogType.ALTER_TRIGGER)
        self.trigger_name = trigger_name
        self.table_name = table_name
        self.timing = timing
        self.events = events
        self.is_row_level = is_row_level
        self.when_condition = when_condition
        self.trigger_body = trigger_body

    def to_bytes(self) -> bytes:
        header = super().to_bytes()
        trigger_name_bytes = self.trigger_name.encode('utf-8')
        table_name_bytes = self.table_name.encode('utf-8')
        timing_bytes = self.timing.encode('utf-8')
        events_json = json.dumps(self.events).encode('utf-8')
        when_bytes = (self.when_condition or '').encode('utf-8')
        body_json = json.dumps(self.trigger_body).encode('utf-8')
        payload = (
            struct.pack('<H', len(trigger_name_bytes)) + trigger_name_bytes +
            struct.pack('<H', len(table_name_bytes)) + table_name_bytes +
            struct.pack('<H', len(timing_bytes)) + timing_bytes +
            struct.pack('<I', len(events_json)) + events_json +
            struct.pack('<?', self.is_row_level) +
            struct.pack('<H', len(when_bytes)) + when_bytes +
            struct.pack('<I', len(body_json)) + body_json
        )
        return header + payload

    @classmethod
    def _from_payload(cls, data: bytes) -> 'LogRecord':
        header_size = cls.HEADER_SIZE
        lsn, prev_lsn, txn_id, _ = struct.unpack(cls.HEADER_FORMAT, data[:header_size])
        offset = header_size
        tname_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        trigger_name = data[offset:offset+tname_len].decode('utf-8')
        offset += tname_len
        tbl_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        table_name = data[offset:offset+tbl_len].decode('utf-8')
        offset += tbl_len
        timing_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        timing = data[offset:offset+timing_len].decode('utf-8')
        offset += timing_len
        events_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        events = json.loads(data[offset:offset+events_len].decode('utf-8'))
        offset += events_len
        is_row_level = struct.unpack('<?', data[offset:offset+1])[0]
        offset += 1
        when_len = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        when_condition = data[offset:offset+when_len].decode('utf-8')
        offset += when_len
        body_len = struct.unpack('<I', data[offset:offset+4])[0]
        offset += 4
        trigger_body = json.loads(data[offset:offset+body_len].decode('utf-8'))
        record = cls(txn_id, trigger_name, table_name, timing, events, is_row_level, when_condition, trigger_body)
        record.lsn = lsn
        record.prev_lsn = prev_lsn
        return record

class LogManager:
    """实现基于 ARIES 思想的预写日志 (WAL) 机制"""
    def __init__(self, log_file_path: str,storage_engine: 'RealStorageEngine'):
        self._log_file_path = log_file_path
        self._log_file: BinaryIO = open(log_file_path, 'ab+')
        self._log_buffer = bytearray()
        self._storage_engine: 'RealStorageEngine' = storage_engine
        self._next_lsn = 1
        self._flushed_lsn = 0
        self._lock = threading.Lock()

        self._log_file.seek(0)
        last_lsn = 0
        
        try:
            # 使用 _read_log_records_from_file 生成器来读取所有记录
            for record in self._read_log_records_from_file():
                if record and record.lsn > last_lsn:
                    last_lsn = record.lsn
        except struct.error:
            # 日志文件末尾可能有损坏的记录，忽略它们
            logger.warning("日志文件可能在末尾损坏，已扫描到最后一个完整记录。")
            pass

        self._next_lsn = last_lsn + 1
        logger.debug(f"_next_lsn:{self._next_lsn}")
        self._flushed_lsn = last_lsn
        
        # ... 恢复所需的内存结构 ...
        self._active_transaction_table: Dict[int, int] = {}
        self._dirty_page_table: Dict[int, int] = {}

    def append(self, transaction: Transaction, log_record: LogRecord) -> int:
        with self._lock:
            log_record.lsn = self._next_lsn
            self._next_lsn += 1
            log_record.prev_lsn = transaction.last_lsn
            transaction.last_lsn = log_record.lsn
            self._log_buffer.extend(log_record.to_bytes_with_prefix())
            logger.debug(f"log_type:{log_record.log_type}-log_record.lsn:{log_record.lsn}")

            return log_record.lsn

    def flush_to_lsn(self, lsn: int):
        with self._lock:
            if lsn <= self._flushed_lsn:
                return
            flush_len = len(self._log_buffer)
            if flush_len == 0:
                return
            bytes_to_flush = self._log_buffer
            self._log_buffer = bytearray()
        
        self._log_file.write(bytes_to_flush)
        self._log_file.flush()
        with self._lock:
            self._flushed_lsn = lsn
            
    def get_flushed_lsn(self) -> int:
        with self._lock:
            return self._flushed_lsn

    def _read_log_records_from_file(self, start_offset=0) -> Generator[LogRecord, None, None]:
        """从日志文件中顺序读取所有日志记录的生成器"""
        self._log_file.seek(start_offset)
        while True:
            len_prefix_bytes = self._log_file.read(4)
            if not len_prefix_bytes:
                break
            
            record_len = struct.unpack('<I', len_prefix_bytes)[0]
            record_bytes = self._log_file.read(record_len)
            
            yield LogRecord.from_bytes(record_bytes)

    def read_log_record_by_lsn(self, lsn: int):
        """根据LSN查找并返回对应的LogRecord对象。"""
        # 简单实现：顺序扫描日志文件，找到目标LSN
        self._log_file.seek(0)
        while True:
            len_prefix_bytes = self._log_file.read(4)
            if not len_prefix_bytes or len(len_prefix_bytes) < 4:
                break
            record_len = struct.unpack('<I', len_prefix_bytes)[0]
            record_bytes = self._log_file.read(record_len)
            if not record_bytes or len(record_bytes) < LogRecord.HEADER_SIZE:
                break
            record = LogRecord.from_bytes(record_bytes)
            if record and getattr(record, 'lsn', None) == lsn:
                return record
        return None

    def recover(self):
        """
        在数据库启动时执行恢复（高效内存优化版，支持超大日志文件，利用检查点）。
        """
        logger.debug("======== 开始数据库恢复 ========")
        max_transaction_id = 0
        # 1. 先顺序扫描日志，找到所有EndCheckpointLogRecord，记录每条日志的LSN和文件偏移
        lsn_to_file_offset_map = {}
        checkpoint_offsets = []  # (file_offset, lsn)
        file_path = self._log_file_path
        with open(file_path, 'rb') as f:
            file_offset = 0
            while True:
                len_prefix_bytes = f.read(4)
                if not len_prefix_bytes or len(len_prefix_bytes) < 4:
                    break
                record_len = struct.unpack('<I', len_prefix_bytes)[0]
                record_bytes = f.read(record_len)
                if not record_bytes or len(record_bytes) < LogRecord.HEADER_SIZE:
                    break
                record = LogRecord.from_bytes(record_bytes)
                if record is None:
                    file_offset += 4 + record_len
                    continue
                lsn_to_file_offset_map[record.lsn] = file_offset
                if isinstance(record, EndCheckpointLogRecord):
                    checkpoint_offsets.append((file_offset, record.lsn, record))
                file_offset += 4 + record_len
        # 2. 找到最后一个EndCheckpointLogRecord，确定分析起点
        if checkpoint_offsets:
            ckpt_offset, ckpt_lsn, ckpt_record = checkpoint_offsets[-1]
            logger.debug(f"[恢复] 检查点: LSN={ckpt_lsn}, 文件偏移={ckpt_offset}")
            att = dict(ckpt_record.att)
            dpt = dict(ckpt_record.dpt)
            scan_start_offset = ckpt_offset + 4 + (len(json.dumps({'att': ckpt_record.att, 'dpt': ckpt_record.dpt}).encode('utf-8')) + LogRecord.HEADER_SIZE)
        else:
            att = {}
            dpt = {}
            scan_start_offset = 0
        # 3. 分析阶段，从检查点后顺序扫描，更新ATT和DPT
        with open(file_path, 'rb') as f:
            f.seek(scan_start_offset)
            file_offset = scan_start_offset
            while True:
                pos = f.tell()
                len_prefix_bytes = f.read(4)
                if not len_prefix_bytes or len(len_prefix_bytes) < 4:
                    break
                record_len = struct.unpack('<I', len_prefix_bytes)[0]
                record_bytes = f.read(record_len)
                if not record_bytes or len(record_bytes) < LogRecord.HEADER_SIZE:
                    break
                record = LogRecord.from_bytes(record_bytes)
                logger.debug(f"[DEBUG][recover] offset={file_offset} lsn={getattr(record, 'lsn', None)} type={getattr(record, 'log_type', None)} txn_id={getattr(record, 'transaction_id', None)} record={record}")
                if record is None:
                    file_offset += 4 + record_len
                    continue
                # --- DDL 日志重建目录 ---
                if getattr(record, 'log_type', None) == LogType.CREATE_TABLE:
                    table_name = record.table_name
                    schema_list = record.schema
                    columns = [ColumnInfo(c[0], c[1]) for c in schema_list]
                    logger.debug(f"【logmanager-recover()】：{table_name}")
                    self._storage_engine.catalog_manager.tables[table_name] = TableInfo(
                        table_name, columns, file_name=f"{table_name}.db"
                    )
                    logger.debug(f"[恢复-分析] 重建表: {table_name}")
                elif getattr(record, 'log_type', None) == LogType.CREATE_INDEX:
                    # 恢复索引元数据
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    file_name = f"{record.table_name}_{record.index_name}.idx"
                    self._storage_engine.catalog_manager.create_index(
                        transaction=recovery_txn,
                        table_name=record.table_name,
                        index_name=record.index_name,
                        column_names=record.columns,
                        file_name=file_name,
                        key_col_types=record.key_col_types,
                        is_unique=record.is_unique
                    )
                    logger.debug(f"[恢复-分析] 重建索引: {record.index_name} on {record.table_name}")
                elif getattr(record, 'log_type', None) == LogType.DROP_INDEX:
                    # 恢复索引删除
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    try:
                        self._storage_engine.catalog_manager.get_index_info(record.table_name, record.index_name)
                        self._storage_engine.catalog_manager.get_table(record.table_name).indexes.pop(record.index_name, None)
                        logger.debug(f"[恢复-分析] 删除索引: {record.index_name} on {record.table_name}")
                    except Exception:
                        pass
                elif getattr(record, 'log_type', None) == LogType.CREATE_VIEW:
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    self._storage_engine.catalog_manager.create_view(
                        transaction=recovery_txn,
                        view_name=record.view_name,
                        definition=record.definition,
                        is_updatable=record.is_updatable
                    )
                    logger.debug(f"[恢复-分析] 重建视图: {record.view_name}")
                elif getattr(record, 'log_type', None) == LogType.ALTER_VIEW:
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    self._storage_engine.catalog_manager.update_view(
                        view_name=record.view_name,
                        definition=record.definition,
                        is_updatable=record.is_updatable
                    )
                    logger.debug(f"[恢复-分析] 修改视图: {record.view_name}")
                elif getattr(record, 'log_type', None) == LogType.DROP_VIEW:
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    if hasattr(self._storage_engine.catalog_manager, 'view_exists') and self._storage_engine.catalog_manager.view_exists(record.view_name):
                        self._storage_engine.catalog_manager.delete_view(
                            transaction=recovery_txn,
                            view_name=record.view_name
                        )
                        logger.debug(f"[恢复-分析] 删除视图: {record.view_name}")
                elif getattr(record, 'log_type', None) == LogType.CREATE_TRIGGER:
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    self._storage_engine.catalog_manager.create_trigger(
                        trigger_name=record.trigger_name,
                        table_name=record.table_name,
                        timing=record.timing,
                        events=record.events,
                        is_row_level=record.is_row_level,
                        when_condition=record.when_condition,
                        trigger_body=record.trigger_body
                    )
                    logger.debug(f"[恢复-分析] 重建触发器: {record.trigger_name}")
                elif getattr(record, 'log_type', None) == LogType.ALTER_TRIGGER:
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    # 这里假设alter就是先删后建（或直接覆盖）
                    self._storage_engine.catalog_manager.create_trigger(
                        trigger_name=record.trigger_name,
                        table_name=record.table_name,
                        timing=record.timing,
                        events=record.events,
                        is_row_level=record.is_row_level,
                        when_condition=record.when_condition,
                        trigger_body=record.trigger_body
                    )
                    logger.debug(f"[恢复-分析] 修改触发器: {record.trigger_name}")
                elif getattr(record, 'log_type', None) == LogType.DROP_TRIGGER:
                    recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                    if hasattr(self._storage_engine.catalog_manager, 'trigger_exists') and self._storage_engine.catalog_manager.trigger_exists(record.trigger_name):
                        self._storage_engine.catalog_manager.delete_trigger(record.trigger_name)
                        logger.debug(f"[恢复-分析] 删除触发器: {record.trigger_name}")
                # --- DDL 处理结束 ---
                if getattr(record, 'transaction_id', 0) > 0:
                    att[record.transaction_id] = record.lsn
                if hasattr(record, 'transaction_id') and record.transaction_id > max_transaction_id:
                    max_transaction_id = record.transaction_id    
                if getattr(record, 'log_type', None) in {LogType.UPDATE, LogType.INSERT, LogType.DELETE, LogType.CLR}:
                    page_id = getattr(record, 'resource_id', None)
                    if page_id is not None:
                        page_id = page_id.page_id
                    if page_id not in dpt:
                        dpt[page_id] = record.lsn
                elif getattr(record, 'log_type', None) in {LogType.COMMIT, LogType.ABORT}:
                    att.pop(record.transaction_id, None)
                file_offset += 4 + record_len
        logger.debug(f"[恢复] 分析完成. 失败者事务: {list(att.keys())}, 脏页: {list(dpt.keys())}")
        
        # 4. 重做阶段 (最终修正版)
        min_rec_lsn = min(dpt.values()) if dpt else NULL_LSN
        file_path = self._log_file_path
        if min_rec_lsn != NULL_LSN:
            for lsn in sorted(lsn_to_file_offset_map.keys()):
                if lsn < min_rec_lsn:
                    continue
                offset = lsn_to_file_offset_map[lsn]
                with open(file_path, 'rb') as f:
                    f.seek(offset)
                    len_prefix_bytes = f.read(4)
                    record_len = struct.unpack('<I', len_prefix_bytes)[0]
                    record_bytes = f.read(record_len)
                    record = LogRecord.from_bytes(record_bytes)
                if record is None:
                    continue
                # --- 关键修正点：在 Redo 阶段更新目录元数据 ---
                if getattr(record, 'log_type', None) in {LogType.UPDATE, LogType.INSERT, LogType.DELETE, LogType.CLR}:
                    page_id = record.resource_id.page_id
                    table_name = record.resource_id.table_name
                    catalog = self._storage_engine.catalog_manager
                    if hasattr(catalog, 'table_exists') and catalog.table_exists(table_name):
                        table_info = catalog.get_table(table_name)
                        if getattr(table_info, 'root_page_id', None) is None:
                            table_info.root_page_id = page_id
                        if getattr(table_info, 'last_page_id', None) is None or page_id > table_info.last_page_id:
                            table_info.last_page_id = page_id
                # --- 修正结束 ---
                if getattr(record, 'log_type', None) in {LogType.UPDATE, LogType.INSERT, LogType.DELETE, LogType.CLR}:
                    if record.resource_id.page_id in dpt:
                        
                        # 【核心修复】使用 try...finally 保证页面解钉
                        page = None
                        buffer_pool = self._storage_engine.buffer_pools.get(record.resource_id.table_name)
                        try:
                            page = self._storage_engine.get_page_for_recovery(record.resource_id)
                            # 检查页面的 LSN 是否小于当前日志记录的 LSN
                            if page.get_page_lsn() < record.lsn:
                                logger.debug(f"REDO: LSN {record.lsn} on Page {record.resource_id.page_id}")
                                recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                                record.redo(self._storage_engine, transaction=recovery_txn)
                        except Exception as e:
                            logger.debug(f"REDO 失败: LSN {record.lsn}, 错误: {e}")
                        finally:
                            if page is not None and buffer_pool is not None:
                                buffer_pool.unpin_page(page.page_id, is_dirty=True)
        logger.debug("[恢复] 重做完成.")
        # 5. 撤销阶段，按需seek读取日志
        logger.debug("[恢复] 3. 撤销阶段: 回滚所有失败者事务...")
        to_undo = {txn_id: lsn for txn_id, lsn in att.items()}
        while to_undo:
            # 选最大LSN的事务优先处理
            max_lsn = 0
            target_txn_id = -1
            for txn_id, lsn in to_undo.items():
                if lsn > max_lsn:
                    max_lsn = lsn
                    target_txn_id = txn_id
            if max_lsn == NULL_LSN:
                break
            # seek到该LSN对应的日志
            record_offset = lsn_to_file_offset_map.get(max_lsn)
            if record_offset is None:
                logger.debug(f"[UNDO] 找不到LSN={max_lsn}的日志，跳过")
                to_undo[target_txn_id] = NULL_LSN
                continue
            with open(file_path, 'rb') as f:
                f.seek(record_offset)
                len_prefix_bytes = f.read(4)
                record_len = struct.unpack('<I', len_prefix_bytes)[0]
                record_bytes = f.read(record_len)
                record = LogRecord.from_bytes(record_bytes)
            if record.log_type in {LogType.UPDATE, LogType.INSERT, LogType.DELETE}:
                logger.debug(f"  UNDO: LSN {record.lsn} for Txn {target_txn_id}")
                undo_info = record.get_undo_info()
                clr = CompensationLogRecord(record.transaction_id, record.prev_lsn, record.log_type, undo_info)
                recovery_txn = Transaction(record.transaction_id, IsolationLevel.SERIALIZABLE)
                clr_lsn = self.append(recovery_txn, clr)
                self.flush_to_lsn(clr_lsn)
                clr.redo(self._storage_engine, transaction=recovery_txn)
                to_undo[target_txn_id] = record.prev_lsn
            elif record.log_type == LogType.CLR:
                to_undo[target_txn_id] = record.undo_next_lsn
            if to_undo.get(target_txn_id) == NULL_LSN:
                logger.debug(f"  Txn {target_txn_id} 回滚完成, 写入 Abort 日志")
                abort_record = AbortLogRecord(target_txn_id)
                self.append(Transaction(target_txn_id, IsolationLevel.SERIALIZABLE), abort_record)
                del to_undo[target_txn_id]
        self.flush_to_lsn(self._next_lsn - 1)
        logger.debug("======== 数据库恢复完成 ========")
        return max_transaction_id
    def __del__(self):
        if self._log_file:
            self._log_file.close()