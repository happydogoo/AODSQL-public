"""
B+树页面实现（内部页与叶子页）以及键序列化工具。

遵循项目编码规范：
- 提供模块文档字符串与类型注解
- 保持现有逻辑与缩进，不做行为更改
"""

import struct
from typing import TYPE_CHECKING, List, Tuple, Union
from src.storage.page import BasePage

if TYPE_CHECKING:
    from src.engine.transaction.transaction import Transaction

# B+树页面类型常量
BTREE_INTERNAL = 1
BTREE_LEAF = 2
NULL_PAGE_ID = 0xFFFFFFFF

# 支持的数据类型常量
KEY_TYPE_INT = 1
KEY_TYPE_STR = 2
KEY_TYPE_FLOAT = 3

class KeySerializer:
    """
    负责key的序列化和反序列化，支持int/str/float及复合key。
    """
    @staticmethod
    def serialize_key(key: Union[Tuple[object, ...], int, float, str]) -> bytes:
        """将key(tuple/int/str/float)序列化为字节流"""
        if not isinstance(key, tuple):
            key = (key,)
        result = b''
        for k in key:
            if isinstance(k, int):
                result += struct.pack('B', KEY_TYPE_INT)
                result += struct.pack('H', 4)  # 长度2字节
                result += struct.pack('i', k)
            elif isinstance(k, float):
                result += struct.pack('B', KEY_TYPE_FLOAT)
                result += struct.pack('H', 8)
                result += struct.pack('d', k)
            elif isinstance(k, str):
                b = k.encode('utf-8')
                result += struct.pack('B', KEY_TYPE_STR)
                result += struct.pack('H', len(b))
                result += b
            else:
                raise TypeError(f"不支持的key类型: {type(k)}")
        return result

    @staticmethod
    def deserialize_key(data: bytes, key_col_types: List[int]) -> Tuple[object, ...]:
        """
        从字节流反序列化key，自动解析类型和长度信息。
        :param data: bytes
        :param key_col_types: 列类型列表
        :return: tuple
        """
        offset = 0
        key = []
        for col_type in key_col_types:
            if offset + 3 > len(data):
                raise ValueError("数据长度不足，无法解析key")
            key_type = data[offset]
            length = struct.unpack('H', data[offset+1:offset+3])[0]
            if offset+3+length > len(data):
                raise ValueError("数据长度不足，无法解析key值")
            val_bytes = data[offset+3:offset+3+length]
            if key_type == KEY_TYPE_INT:
                val = struct.unpack('i', val_bytes)[0]
            elif key_type == KEY_TYPE_FLOAT:
                val = struct.unpack('d', val_bytes)[0]
            elif key_type == KEY_TYPE_STR:
                val = val_bytes.decode('utf-8')
            else:
                raise TypeError(f"未知key类型: {key_type}")
            key.append(val)
            offset += 3 + length
        return tuple(key)

    @staticmethod
    def get_key_length_from_bytes(data: bytes, key_col_types: List[int]) -> int:
        """
        从字节流中计算key的总长度（自动解析所有key组件）。
        :param data: bytes
        :param key_col_types: 列类型列表（必须）
        :return: int
        """
        offset = 0
        for col_type in key_col_types:
            if offset + 3 > len(data):
                raise ValueError("数据长度不足，无法解析key长度")
            length = struct.unpack('H', data[offset+1:offset+3])[0]
            if offset+3+length > len(data):
                raise ValueError("数据长度不足，无法解析key值")
            offset += 3 + length
        return offset

    @staticmethod
    def key_length(key: Union[Tuple[object, ...], int, float, str]) -> int:
        """返回序列化后key的总字节长度"""
        return len(KeySerializer.serialize_key(key))

# B+树页面基类
class BTreePageBase(BasePage):
    """
    B+树页面基类，包含通用页头、槽数组、数据区管理。
    独立于堆表页Page。
    """
    # 页头格式: type(1) free_ptr(4) entry_count(4) next_leaf(4) parent_id(4) page_lsn(8)
    HEADER_STRUCT = struct.Struct('B I I I I Q')
    HEADER_SIZE = HEADER_STRUCT.size  # 1+4+4+4+4+4=21字节
    SLOT_STRUCT = struct.Struct('H H')  # offset(2) length(2)
    SLOT_SIZE = SLOT_STRUCT.size

    def __init__(self, page_id, data=None, page_size=4096):
        super().__init__(page_id, data, page_size)
        # 初始化B+树页头相关属性
        if data is not None:
            self._load_header()
        else:
            self.page_type = 0
            self.free_space_pointer = self.HEADER_SIZE
            self.entry_count = 0
            self.next_leaf_page_id = NULL_PAGE_ID
            self.parent_page_id = NULL_PAGE_ID
            self.page_lsn = 0

    def _init_header(self):
        self.page_type = 0
        self.page_id = self.page_id
        self.free_space_pointer = self.HEADER_SIZE
        self.entry_count = 0
        self.next_leaf_page_id = NULL_PAGE_ID
        self.parent_page_id = NULL_PAGE_ID
        self._save_header()

    def _load_header(self):
        (self.page_type, self.free_space_pointer, self.entry_count,
         self.next_leaf_page_id, self.parent_page_id, self.page_lsn) = self.HEADER_STRUCT.unpack(self.data[:self.HEADER_SIZE])

    def _save_header(self):
        self.data[:self.HEADER_SIZE] = self.HEADER_STRUCT.pack(
            self.page_type, self.free_space_pointer, self.entry_count,
            self.next_leaf_page_id, self.parent_page_id, self.page_lsn)

    def get_page_lsn(self) -> int:
        return self.page_lsn

    def set_page_lsn(self, lsn: int):
        self.page_lsn = lsn
        self.is_dirty = True
        self._save_header()

    def get_free_space(self) -> int:
        """返回当前页面剩余可用空间（字节数）"""
        slot_array_start = self.page_size - self.entry_count * self.SLOT_SIZE
        return slot_array_start - self.free_space_pointer

    def _get_slot_offset(self, idx: int) -> int:
        """返回第idx个槽的起始偏移（在页面中的位置）"""
        return self.page_size - (idx + 1) * self.SLOT_SIZE

    def _read_slot(self, idx: int) -> Tuple[int, int]:
        """读取第idx个槽，返回(offset, length)"""
        slot_off = self._get_slot_offset(idx)
        return self.SLOT_STRUCT.unpack(self.data[slot_off:slot_off+self.SLOT_SIZE])

    def _write_slot(self, idx: int, data_offset: int, data_len: int) -> None:
        slot_off = self._get_slot_offset(idx)
        self.data[slot_off:slot_off+self.SLOT_SIZE] = self.SLOT_STRUCT.pack(data_offset, data_len)

    def _find_slot_for_key(self, key: Tuple[object, ...], is_leaf: bool = True) -> int:
        """
        二分查找，返回key应该插入的位置idx。
        循环中反序列化key进行比较，保证正确性。
        :param key: tuple
        :param is_leaf: 是否为叶子页
        :return: int
        """

        print("keykeykey:",key)
        low, high = 0, self.entry_count - 1
        while low <= high:
            mid = (low + high) // 2
            off, length = self._read_slot(mid)
            entry_bytes = self.data[off:off+length]
            # 只取key部分
            print("self.key_col_types:",self.key_col_types)
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            if key_len == 0 or key_len > len(entry_bytes):
                mid_key = ()
            else:
                slot_key_bytes = entry_bytes[:key_len]
                mid_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
            print(f"[BTree DEBUG] Comparing SEARCH_KEY={repr(key)} ({type(key[0])}) WITH PAGE_KEY={repr(mid_key)} ({type(mid_key[0]) if mid_key else 'N/A'})")
            if mid_key == key:
                return mid  # 精确匹配
            elif mid_key < key:
                low = mid + 1
            else:
                high = mid - 1
        return low  # 未找到，返回插入点

    def _insert_data(self, data_bytes: bytes, insert_offset: int) -> int:
        """
        在数据区的insert_offset处插入字节，移动后续数据，返回插入的偏移。
        只负责物理移动和槽offset更新，不更新free_space_pointer和entry_count。
        :param data_bytes: 要插入的数据
        :param insert_offset: 插入点在数据区的偏移
        :return: 实际插入的偏移
        """
        data_len = len(data_bytes)
        tail_len = self.free_space_pointer - insert_offset
        if tail_len > 0:
            self.data[insert_offset+data_len:self.free_space_pointer+data_len] = self.data[insert_offset:self.free_space_pointer]
        self.data[insert_offset:insert_offset+data_len] = data_bytes
        for i in range(self.entry_count):
            off, length = self._read_slot(i)
            if off >= insert_offset:
                self._write_slot(i, off+data_len, length)
        return insert_offset

    def _delete_data(self, delete_offset: int, delete_len: int) -> None:
        """
        删除数据区delete_offset处长度为delete_len的字节，移动后续数据，更新槽offset。
        只负责物理移动和槽offset更新，不更新free_space_pointer和entry_count。
        """
        tail_len = self.free_space_pointer - (delete_offset + delete_len)
        if tail_len > 0:
            self.data[delete_offset:delete_offset+tail_len] = self.data[delete_offset+delete_len:self.free_space_pointer]
        for i in range(self.entry_count):
            off, length = self._read_slot(i)
            if off > delete_offset:
                self._write_slot(i, off-delete_len, length)

    def _compact(self) -> None:
        """
        整理页面碎片，紧凑存储所有数据。只在最后调用一次_save_header。
        """
        entries = []
        for i in range(self.entry_count):
            off, length = self._read_slot(i)
            entries.append(self.data[off:off+length])
        self.free_space_pointer = self.HEADER_SIZE
        for i, entry in enumerate(entries):
            off = self._insert_data(entry, self.free_space_pointer)
            self._write_slot(i, off, len(entry))
            self.free_space_pointer += len(entry)
        self._save_header()

    def _delete_slot(self, idx: int) -> None:
        """
        删除第idx个槽，并移动后续槽。
        只负责物理移动，不更新entry_count和free_space_pointer。
        """
        for i in range(idx, self.entry_count - 1):
            src_off = self._get_slot_offset(i+1)
            dst_off = self._get_slot_offset(i)
            self.data[dst_off:dst_off+self.SLOT_SIZE] = self.data[src_off:src_off+self.SLOT_SIZE]
        # 可选：清空最后一个槽
        # last_off = self._get_slot_offset(self.entry_count - 1)
        # self.data[last_off:last_off+self.SLOT_SIZE] = b'\x00' * self.SLOT_SIZE

    @property
    def min_size(self) -> int:
        """
        返回页面最小安全entry数，通常为最大容量的一半。
        """
        # 估算最大entry数
        max_entry = (self.page_size - self.HEADER_SIZE) // (32 + self.SLOT_SIZE) # 32为典型entry估算
        return max(1, max_entry // 2)


class BTreeInternalPage(BTreePageBase):
    """
    B+树内部节点页，存储N个key和N+1个子节点指针。
    采用页头leftmost_child_page_id + 槽数组(key, child_page_id)的正统结构。
    """
    # 页头格式: type(1) page_id(4) free_ptr(4) entry_count(4) next_leaf(4) parent_id(4) leftmost_child(4)
    HEADER_STRUCT = struct.Struct('B I I I I Q I')  # 比父类多一个leftmost_child_page_id
    HEADER_SIZE = HEADER_STRUCT.size

    def __init__(self, page_id, data=None, page_size=4096, key_col_types=None):
        super().__init__(page_id, data, page_size)
        self.key_col_types = key_col_types or [KEY_TYPE_INT]
        if data is not None:
            self._load_header()
        else:
            self.page_type = BTREE_INTERNAL
            self.free_space_pointer = self.HEADER_SIZE
            self.entry_count = 0
            self.next_leaf_page_id = NULL_PAGE_ID
            self.parent_page_id = NULL_PAGE_ID
            self.leftmost_child_page_id = NULL_PAGE_ID  # 必须初始化！
            self._save_header()

    def _load_header(self):
        (self.page_type, self.free_space_pointer, self.entry_count,
         self.next_leaf_page_id, self.parent_page_id, self.page_lsn,
         self.leftmost_child_page_id) = self.HEADER_STRUCT.unpack(self.data[:self.HEADER_SIZE])

    def _save_header(self):
        self.data[:self.HEADER_SIZE] = self.HEADER_STRUCT.pack(
            self.page_type, self.free_space_pointer, self.entry_count,
            self.next_leaf_page_id, self.parent_page_id, self.page_lsn,
            self.leftmost_child_page_id)

    def insert(self, transaction: 'Transaction', key, child_page_id, key_col_types, lsn: int):
        """
        在当前内部页中插入(key, child_page_id)。
        :param key: tuple，插入的key
        :param child_page_id: int，插入的右侧子节点page_id
        :param key_col_types: 列类型列表
        :return: True表示插入成功，False表示空间不足需分裂
        """
        idx = self._find_slot_for_key(key, False)
        key_bytes = KeySerializer.serialize_key(key)
        child_bytes = struct.pack('I', child_page_id)
        entry_bytes = key_bytes + child_bytes
        entry_len = len(entry_bytes)
        if self.get_free_space() < entry_len + self.SLOT_SIZE:
            self._compact()
            if self.get_free_space() < entry_len + self.SLOT_SIZE:
                return False
        data_off = self.free_space_pointer
        self.data[data_off:data_off+entry_len] = entry_bytes
        for i in range(self.entry_count, idx, -1):
            src_off = self._get_slot_offset(i-1)
            dst_off = self._get_slot_offset(i)
            self.data[dst_off:dst_off+self.SLOT_SIZE] = self.data[src_off:src_off+self.SLOT_SIZE]
        self._write_slot(idx, data_off, entry_len)
        self.entry_count += 1
        self.free_space_pointer += entry_len
        self._save_header()
        self.set_page_lsn(lsn)
        return True

    def split(self, transaction: 'Transaction', new_page_id, lsn: int):
        """
        分裂内部节点，将一半数据移动到新页。
        :param new_page_id: 新右兄弟页的page_id
        :return: (上推key, new_right_page)  # new_right_page为BTreeInternalPage实例
        """
        mid = self.entry_count // 2
        # 1. 提取中间key和P_mid
        off, length = self._read_slot(mid)
        entry_bytes = self.data[off:off+length]
        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
        up_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
        p_mid = struct.unpack('I', entry_bytes[key_len:key_len+4])[0]
        # 2. 创建新右兄弟页，设置leftmost_child_page_id为P_mid
        new_right = BTreeInternalPage(new_page_id, page_size=self.page_size, key_col_types=self.key_col_types)
        new_right.leftmost_child_page_id = p_mid
        new_right._save_header()
        # 3. 将mid+1及以后的entry全部插入到new_right
        for i in range(mid+1, self.entry_count):
            off_i, length_i = self._read_slot(i)
            entry_data_i = self.data[off_i:off_i+length_i]
            key_len_i = KeySerializer.get_key_length_from_bytes(entry_data_i, self.key_col_types)
            key_i = KeySerializer.deserialize_key(entry_data_i[:key_len_i], self.key_col_types)
            child_id_i = struct.unpack('I', entry_data_i[key_len_i:])[0]
            # 修正：传递所有必需参数
            new_right.insert(transaction, key_i, child_id_i, self.key_col_types, lsn)
        # 4. 截断原节点
        self.entry_count = mid
        self._save_header()
        self.set_page_lsn(lsn)
        new_right.set_page_lsn(lsn)
        return up_key, new_right

    def delete(self, transaction: 'Transaction', key, key_col_types, lsn: int):
        """
        从当前内部页中删除(key, child_page_id)对（只根据key删除目录项）。
        :param key: tuple，待删除的key
        :param key_col_types: 列类型列表
        :return: bool，是否删除成功
        """
        idx = self._find_slot_for_key(key, False)
        if idx < self.entry_count:
            off, length = self._read_slot(idx)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, key_col_types)
            slot_key_bytes = entry_bytes[:key_len]
            slot_key = KeySerializer.deserialize_key(slot_key_bytes, key_col_types)
            if slot_key == key:
                self._delete_slot(idx)
                self.entry_count -= 1
                self._save_header()
                self.set_page_lsn(lsn)
                return True
        return False

    def delete_entry_by_index(self, idx):
        """
        按槽位索引删除目录项。
        """
        self._delete_slot(idx)
        self.entry_count -= 1
        self._save_header()

    def get_leftmost_child(self):
        return self.leftmost_child_page_id

    def set_leftmost_child(self, page_id):
        self.leftmost_child_page_id = page_id
        self._save_header()

    # --- 私有化原有实现 ---
    def _insert_logic(self, key, child_page_id, key_col_types):
        idx = self._find_slot_for_key(key, False)
        key_bytes = KeySerializer.serialize_key(key)
        child_bytes = struct.pack('I', child_page_id)
        entry_bytes = key_bytes + child_bytes
        entry_len = len(entry_bytes)
        if self.get_free_space() < entry_len + self.SLOT_SIZE:
            self._compact()
            if self.get_free_space() < entry_len + self.SLOT_SIZE:
                return False
        data_off = self.free_space_pointer
        self.data[data_off:data_off+entry_len] = entry_bytes
        for i in range(self.entry_count, idx, -1):
            src_off = self._get_slot_offset(i-1)
            dst_off = self._get_slot_offset(i)
            self.data[dst_off:dst_off+self.SLOT_SIZE] = self.data[src_off:src_off+self.SLOT_SIZE]
        self._write_slot(idx, data_off, entry_len)
        self.entry_count += 1
        self.free_space_pointer += entry_len
        self._save_header()
        return True

    def _split_logic(self, new_page_id):
        mid = self.entry_count // 2
        off, length = self._read_slot(mid)
        entry_bytes = self.data[off:off+length]
        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
        up_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
        p_mid = struct.unpack('I', entry_bytes[key_len:key_len+4])[0]
        new_right = BTreeInternalPage(new_page_id, page_size=self.page_size, key_col_types=self.key_col_types)
        new_right.leftmost_child_page_id = p_mid
        new_right._save_header()
        for i in range(mid+1, self.entry_count):
            off_i, length_i = self._read_slot(i)
            entry_data_i = self.data[off_i:off_i+length_i]
            key_len_i = KeySerializer.get_key_length_from_bytes(entry_data_i, self.key_col_types)
            key_i = KeySerializer.deserialize_key(entry_data_i[:key_len_i], self.key_col_types)
            child_id_i = struct.unpack('I', entry_data_i[key_len_i:])[0]
            new_right._insert_logic(key_i, child_id_i, self.key_col_types)
        self.entry_count = mid
        self._save_header()
        return up_key, new_right

    def _delete_logic(self, key, key_col_types):
        idx = self._find_slot_for_key(key, False)
        if idx < self.entry_count:
            off, length = self._read_slot(idx)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, key_col_types)
            slot_key_bytes = entry_bytes[:key_len]
            slot_key = KeySerializer.deserialize_key(slot_key_bytes, key_col_types)
            if slot_key == key:
                self._delete_slot(idx)
                self.entry_count -= 1
                self._save_header()
                return True
        return False

class BTreeLeafPage(BTreePageBase):
    """
    B+树叶子节点页，存储key和row_id。
    支持多列key（tuple）。
    """
    def __init__(self, page_id, data=None, page_size=4096, key_col_types=None):
        super().__init__(page_id, data, page_size)
        self.key_col_types = key_col_types or [KEY_TYPE_INT]
        if data is None:
            self.page_type = BTREE_LEAF
            self._save_header()

    def search(self, key, key_col_types):
        """
        在当前叶子页中查找key对应的row_id。
        :param key: tuple，查找的key
        :param key_col_types: 列类型列表
        :return: row_id (tuple) 或 None
        """
        idx = self._find_slot_for_key(key, True) # 调用父类的_find_slot_for_key，并指定is_leaf=True
        # 检查idx位置的key是否等于目标key
        print("查找到idx：",idx,"self.entry_count:",self.key_col_types,idx < self.entry_count)
        if idx < self.entry_count:

            off, length = self._read_slot(idx)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            print("key_len:",key_len,len(entry_bytes))
            if key_len == 0 or key_len > len(entry_bytes):
                slot_key = ()
            else:
                slot_key_bytes = entry_bytes[:key_len]
                slot_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
                print("slot_key:",slot_key)
            if slot_key == key:
                # row_id紧跟在key后面，占8字节
                row_id_bytes = entry_bytes[key_len:key_len+8]
                page_id, record_id = struct.unpack('II', row_id_bytes)
                print(page_id,record_id)
                return (page_id, record_id)
        return None

    def insert(self, transaction: 'Transaction', key, row_id, key_col_types, lsn: int, is_unique=False):
        """
        在当前叶子页中插入(key, row_id)。支持唯一性约束。
        :param key: tuple，插入的key
        :param row_id: tuple (page_id, record_id)
        :param key_col_types: 列类型列表
        :param is_unique: 是否唯一索引
        :return: True表示插入成功，False表示空间不足需分裂
        :raises: ValueError 唯一性冲突
        """
        # 唯一性检查
        idx = self._find_slot_for_key(key, True)
        print("插入key：",key)
        if is_unique and idx < self.entry_count:
            off, length = self._read_slot(idx)
            entry_bytes = self.data[off:off+length]

            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            if key_len == 0 or key_len > len(entry_bytes):
                slot_key = ()
            else:
                slot_key_bytes = entry_bytes[:key_len]
                slot_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
            if slot_key == key:
                raise ValueError("唯一索引冲突：该key已存在")
        # 序列化key和row_id
        key_bytes = KeySerializer.serialize_key(key)
        row_id_bytes = struct.pack('II', row_id[0], row_id[1])
        entry_bytes = key_bytes + row_id_bytes
        entry_len = len(entry_bytes)
        # 检查空间
        if self.get_free_space() < entry_len + self.SLOT_SIZE:
            # 空间不足，尝试compact
            self._compact()
            if self.get_free_space() < entry_len + self.SLOT_SIZE:
                return False  # 仍不足，需分裂
        # 数据区追加
        data_off = self.free_space_pointer
        self.data[data_off:data_off+entry_len] = entry_bytes
        # 槽数组后移，为新槽腾出空间
        for i in range(self.entry_count, idx, -1):
            src_off = self._get_slot_offset(i-1)
            dst_off = self._get_slot_offset(i)
            self.data[dst_off:dst_off+self.SLOT_SIZE] = self.data[src_off:src_off+self.SLOT_SIZE]
        # 写新槽
        self._write_slot(idx, data_off, entry_len)
        self.entry_count += 1
        self.free_space_pointer += entry_len
        self._save_header()
        self.set_page_lsn(lsn)
        return True

    def delete(self, transaction: 'Transaction', key, row_id, key_col_types, lsn: int):
        """
        从当前叶子页中删除(key, row_id)对。
        只移动槽数组，不移动数据区。
        :param key: tuple，待删除的key
        :param row_id: tuple (page_id, record_id)
        :param key_col_types: 列类型列表
        :return: (bool, bool) -> (是否删除成功, 是否过空)
        """
        idx = self._find_slot_for_key(key, True)
        if idx < self.entry_count:
            off, length = self._read_slot(idx)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            if key_len == 0 or key_len > len(entry_bytes):
                slot_key = ()
            else:
                slot_key_bytes = entry_bytes[:key_len]
                slot_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
            if slot_key == key:
                row_id_bytes = entry_bytes[key_len:key_len+8]
                page_id, record_id = struct.unpack('II', row_id_bytes)
                if (page_id, record_id) == row_id:
                    self._delete_slot(idx)
                    self.entry_count -= 1
                    # free_space_pointer不变，数据区不动
                    self._save_header()
                    is_underflow = self.entry_count < max(1, (self.page_size // 2) // (length + self.SLOT_SIZE))
                    self.set_page_lsn(lsn)
                    return True, is_underflow
        self.set_page_lsn(lsn)
        return False, False

    def split(self, transaction: 'Transaction', new_right_page, lsn: int):
        """
        分裂叶子节点，将一半数据移动到new_right_page，并维护链表指针。
        :param new_right_page: 由buffer pool分配的新右兄弟页对象
        :return: 上推key（new_right_page的最小key）
        """
        mid = self.entry_count // 2
        # 1. 将mid及以后的entry全部插入到new_right_page
        old_entry_count = self.entry_count
        for i in range(mid, old_entry_count):
            off, length = self._read_slot(i)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            if key_len == 0 or key_len > len(entry_bytes):
                slot_key = ()
            else:
                slot_key_bytes = entry_bytes[:key_len]
                slot_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
            row_id_bytes = entry_bytes[key_len:key_len+8]
            page_id, record_id = struct.unpack('II', row_id_bytes)
            # 修正：传递所有必需参数
            new_right_page.insert(transaction, slot_key, (page_id, record_id), self.key_col_types, lsn)
        new_right_page.next_leaf_page_id = self.next_leaf_page_id
        self.next_leaf_page_id = new_right_page.page_id
        # 2. 截断原节点
        self.entry_count = mid
        self._save_header()
        # 3. 返回new_right_page的最小key
        first_off, first_len = new_right_page._read_slot(0)
        first_entry = new_right_page.data[first_off:first_off+first_len]
        key_len = KeySerializer.get_key_length_from_bytes(first_entry, self.key_col_types)
        if key_len == 0 or key_len > len(first_entry):
            min_key = ()
        else:
            min_key_bytes = first_entry[:key_len]
            min_key = KeySerializer.deserialize_key(min_key_bytes, self.key_col_types)
        self.set_page_lsn(lsn)
        new_right_page.set_page_lsn(lsn)
        return min_key

    # --- 私有化原有实现 ---
    def _insert_logic(self, key, row_id, key_col_types, is_unique=False):
        idx = self._find_slot_for_key(key, True)
        if is_unique and idx < self.entry_count:
            off, length = self._read_slot(idx)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            slot_key_bytes = entry_bytes[:key_len]
            slot_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
            if slot_key == key:
                raise ValueError("唯一索引冲突：该key已存在")
        key_bytes = KeySerializer.serialize_key(key)
        row_id_bytes = struct.pack('II', row_id[0], row_id[1])
        entry_bytes = key_bytes + row_id_bytes
        entry_len = len(entry_bytes)
        if self.get_free_space() < entry_len + self.SLOT_SIZE:
            self._compact()
            if self.get_free_space() < entry_len + self.SLOT_SIZE:
                return False
        data_off = self.free_space_pointer
        self.data[data_off:data_off+entry_len] = entry_bytes
        for i in range(self.entry_count, idx, -1):
            src_off = self._get_slot_offset(i-1)
            dst_off = self._get_slot_offset(i)
            self.data[dst_off:dst_off+self.SLOT_SIZE] = self.data[src_off:src_off+self.SLOT_SIZE]
        self._write_slot(idx, data_off, entry_len)
        self.entry_count += 1
        self.free_space_pointer += entry_len
        self._save_header()
        return True

    def _delete_logic(self, key, row_id, key_col_types):
        idx = self._find_slot_for_key(key, True)
        if idx < self.entry_count:
            off, length = self._read_slot(idx)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            slot_key_bytes = entry_bytes[:key_len]
            slot_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
            if slot_key == key:
                row_id_bytes = entry_bytes[key_len:key_len+8]
                page_id, record_id = struct.unpack('II', row_id_bytes)
                if (page_id, record_id) == row_id:
                    self._delete_slot(idx)
                    self.entry_count -= 1
                    self._save_header()
                    is_underflow = self.entry_count < max(1, (self.page_size // 2) // (length + self.SLOT_SIZE))
                    return True, is_underflow
        return False, False

    def _split_logic(self, new_right_page):
        mid = self.entry_count // 2
        old_entry_count = self.entry_count
        for i in range(mid, old_entry_count):
            off, length = self._read_slot(i)
            entry_bytes = self.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            slot_key_bytes = entry_bytes[:key_len]
            slot_key = KeySerializer.deserialize_key(slot_key_bytes, self.key_col_types)
            row_id_bytes = entry_bytes[key_len:key_len+8]
            page_id, record_id = struct.unpack('II', row_id_bytes)
            new_right_page._insert_logic(slot_key, (page_id, record_id), self.key_col_types)
        new_right_page.next_leaf_page_id = self.next_leaf_page_id
        self.next_leaf_page_id = new_right_page.page_id
        self.entry_count = mid
        self._save_header()
        first_off, first_len = new_right_page._read_slot(0)
        first_entry = new_right_page.data[first_off:first_off+first_len]
        key_len = KeySerializer.get_key_length_from_bytes(first_entry, self.key_col_types)
        min_key_bytes = first_entry[:key_len]
        min_key = KeySerializer.deserialize_key(min_key_bytes, self.key_col_types)
        return min_key