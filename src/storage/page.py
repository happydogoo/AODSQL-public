"""
页面抽象与堆页实现。

遵循规范：
- 提供模块/类/方法文档字符串
- 添加关键类型注解
- 不改变现有逻辑或缩进
"""

import struct
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from src.engine.transaction.transaction import Transaction


class BasePage:
    """
    所有页类型的统一基类，提供通用属性和接口。
    """
    def __init__(self, page_id: int, data: Optional[bytes] = None, page_size: int = 4096):
        self.page_id = page_id
        self.page_size = page_size
        if data is not None:
            self.data = bytearray(data)
        else:
            self.data = bytearray(page_size)
        self.is_dirty = False
        self.pin_count = 0
    def to_bytes(self) -> bytes:
        return bytes(self.data)
    def mark_dirty(self) -> None:
        self.is_dirty = True
    def pin(self) -> None:
        self.pin_count += 1
    def unpin(self) -> None:
        if self.pin_count > 0:
            self.pin_count -= 1


class HeapPage(BasePage):

    # 页头格式：is_leaf(1字节), key_count(4字节), next_page_id(4字节), page_lsn(8字节)
    # 1 + 4 + 4 + 8 = 17 字节。为了对齐，我们用 7 字节填充
    HEADER_STRUCT = struct.Struct('B i i Q 7s')  # 1+4+4+8+7=24字节
    HEADER_SIZE = HEADER_STRUCT.size

    def __init__(self, page_id: int, data: Optional[bytes] = None, page_size: int = 4096):
        super().__init__(page_id, data, page_size)
        # B+树相关属性
        self.is_leaf = False
        self.key_count = 0
        self.next_page_id = 0
        self.page_lsn = 0
        self._load_header()

    def _load_header(self) -> None:
        if len(self.data) >= self.HEADER_SIZE:
            is_leaf, key_count, next_page_id, page_lsn, _ = self.HEADER_STRUCT.unpack(self.data[:self.HEADER_SIZE])
            self.is_leaf = bool(is_leaf)
            self.key_count = key_count
            self.next_page_id = next_page_id
            self.page_lsn = page_lsn

    def _save_header(self) -> None:
        header = self.HEADER_STRUCT.pack(int(self.is_leaf), self.key_count, self.next_page_id, self.page_lsn, b'\x00'*7)
        self.data[:self.HEADER_SIZE] = header
    
    def get_page_lsn(self) -> int:
        return self.page_lsn

    def set_page_lsn(self, lsn: int) -> None:
        self.page_lsn = lsn
        self.is_dirty = True # 修改 LSN 也是一种页面变更
        self._save_header()

    def insert_record(self, transaction: 'Transaction', record_data: bytes, lsn: int) -> int:
        record_id = self._insert_record_logic(record_data)
        self.set_page_lsn(lsn)
        return record_id

    def update_record(self, transaction: 'Transaction', record_id: int, record_data: bytes, lsn: int) -> bool:
        record_size = len(record_data)
        updated = self._update_record_logic(record_id, record_data, record_size)
        if updated:
            self.set_page_lsn(lsn)
        return updated

    def mark_as_deleted(self, transaction: 'Transaction', record_id: int, record_size: int, lsn: int) -> bool:
        deleted = self._mark_as_deleted_logic(record_id, record_size)
        if deleted:
            self.set_page_lsn(lsn)
        return deleted

    def insert_record_at(self, transaction: 'Transaction', record_id: int, record_data: bytes, lsn: int) -> None:
        record_size = len(record_data) + 1  # 多1字节有效标记
        offset = self.HEADER_SIZE + record_id * record_size
        # 尝试解析 record_data
        try:
            str_part = record_data.decode('utf-8', errors='ignore')
        except Exception:
            str_part = str(record_data)
        int_part = None
        if len(record_data) >= 4:
            try:
                int_part = int.from_bytes(record_data[-4:], 'little')
            except Exception:
                int_part = None
        if offset + record_size > self.page_size:
            raise ValueError('Page is full, cannot insert record.')
        self.data[offset] = 1
        self.data[offset+1:offset+record_size] = record_data
        if record_id >= self.key_count:
            self.key_count = record_id + 1
        self.set_page_lsn(lsn)
        self.is_dirty = True
        self._save_header()

    def _insert_record_logic(self, record_data: bytes) -> int:
        record_size = len(record_data) + 1  # 多1字节有效标记
        for i in range(self.key_count):
            offset = self.HEADER_SIZE + i * record_size
            if self.data[offset] == 0:
                self.data[offset] = 1
                self.data[offset+1:offset+record_size] = record_data
                self.is_dirty = True
                self._save_header()
                return i
        offset = self.HEADER_SIZE + self.key_count * record_size
        if offset + record_size > self.page_size:
            raise ValueError('Page is full, cannot insert record.')
        self.data[offset] = 1
        self.data[offset+1:offset+record_size] = record_data
        self.key_count += 1
        self.is_dirty = True
        self._save_header()
        return self.key_count - 1

    def _update_record_logic(self, record_id: int, record_data: bytes, record_size: int) -> bool:
        record_size += 1 # 有效位
        if record_id < 0 or record_id >= self.key_count:
            return False
        offset = self.HEADER_SIZE + record_id * record_size
        self.data[offset+1:offset+record_size] = record_data
        self.is_dirty = True
        self._save_header()
        return True

    def _mark_as_deleted_logic(self, record_id: int, record_size: int) -> bool:
        record_size += 1 # 有效位
        if record_id < 0 or record_id >= self.key_count:
            return False
        offset = self.HEADER_SIZE + record_id * record_size
        if self.data[offset] == 0:
            return False  # 已经无效
        self.data[offset] = 0
        self.is_dirty = True
        self._save_header()
        return True
        
    def is_full(self, record_size: int) -> bool:
        """
        检查页是否还有空闲空间。
        """
        record_size += 1
        offset = self.HEADER_SIZE + self.key_count * record_size
        return offset + record_size > self.page_size

    def to_bytes(self) -> bytes:
        """
        将整个页对象序列化成字节数据。
        """
        self._save_header()
        return bytes(self.data)

    @classmethod
    def from_bytes(cls, page_id: int, data: bytes, page_size: int = 4096) -> 'HeapPage':
        """
        从字节数据反序列化为Page对象。
        """
        return cls(page_id, data, page_size)

    def __repr__(self) -> str:
        return (f"<Page id={self.page_id} size={len(self.data)} bytes "
                f"dirty={self.is_dirty} pin={self.pin_count} leaf={self.is_leaf} keys={self.key_count} next={self.next_page_id}>")

    def get_record(self, record_id: int, record_size: int):
        offset = self.HEADER_SIZE + record_id * (record_size + 1)
        if offset + record_size + 1 > self.page_size:
            return False, None
        valid = self.data[offset] == 1
        if not valid:
            return False, None
        row_bytes = bytes(self.data[offset+1:offset+1+record_size])
        return True, row_bytes
