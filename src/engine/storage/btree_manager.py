from src.storage.buffer import BufferPool
from src.engine.catalog_manager import CatalogManager
from src.storage.btreepage import BTreeLeafPage, BTreeInternalPage, BTREE_LEAF, BTREE_INTERNAL
from typing import Optional, TYPE_CHECKING
from src.storage.btreepage import KeySerializer,NULL_PAGE_ID
import struct

if TYPE_CHECKING:
    from src.engine.transaction import Transaction

class BTreeManager:
    def __init__(self, buffer_pool: BufferPool, catalog_manager: CatalogManager, 
                 table_name: str, index_name: str, key_col_types: list):
        """
        初始化一个 B+ 树索引管理器。
        :param buffer_pool: 该索引对应表的缓冲池。
        :param catalog_manager: 全局目录管理器。
        :param table_name: 索引所属的表名。
        :param index_name: 索引名，用于从目录获取root_page_id。
        :param key_col_types: 索引键的类型列表。
        """
        self.bp = buffer_pool
        self.cm = catalog_manager
        self.table_name = table_name
        self.index_name = index_name
        self.key_col_types = key_col_types
        # 从目录中获取根节点ID（简化，实际应为 table_info.indexes[index_name].root_page_id）
        table_info = self.cm.get_table(self.table_name)
        idx_info = table_info.indexes.get(self.index_name)
        if not idx_info:
            raise ValueError(f"索引 {self.index_name} 在表 {self.table_name} 的目录中未找到")
        self.root_page_id = idx_info.root_page_id
        # 获取唯一性属性
        self.is_unique = idx_info.is_unique if idx_info else False

    def _get_page(self, page_id: int):
        """辅助方法，从缓冲池获取页面并包装成正确的B+树页面对象。"""
        # 直接用BufferPool的get_page，传递正确的page_cls和key_col_types
        page_data = self.bp.get_page(page_id, page_cls=BTreeLeafPage, key_col_types=self.key_col_types)
        page_type = page_data.page_type if hasattr(page_data, 'page_type') else page_data.data[0]
        if page_type == BTREE_LEAF:
            return self.bp.get_page(page_id, page_cls=BTreeLeafPage, key_col_types=self.key_col_types)
        elif page_type == BTREE_INTERNAL:
            return self.bp.get_page(page_id, page_cls=BTreeInternalPage, key_col_types=self.key_col_types)
        else:
            self.bp.unpin_page(page_id, is_dirty=False)
            raise TypeError(f"未知的页面类型: {page_type}")

    def _find_internal_child_page_id(self, internal_page: BTreeInternalPage, key: tuple) -> int:
        """
        在内部节点中查找key应该走的子指针。
        :param internal_page: BTreeInternalPage实例
        :param key: 查找的key
        :return: 下一个子页面id
        """
        if internal_page.entry_count == 0:
            return internal_page.get_leftmost_child()
        # 遍历所有槽，找到第一个大于key的槽，返回其左侧child
        for i in range(internal_page.entry_count):
            off, length = internal_page._read_slot(i)
            entry_bytes = internal_page.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            slot_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
            if key < slot_key:
                if i == 0:
                    return internal_page.get_leftmost_child()
                else:
                    off_l, length_l = internal_page._read_slot(i-1)
                    entry_bytes_l = internal_page.data[off_l:off_l+length_l]
                    key_len_l = KeySerializer.get_key_length_from_bytes(entry_bytes_l, self.key_col_types)
                    child_page_id = struct.unpack('I', entry_bytes_l[key_len_l:])[0]
                    return child_page_id
        # 如果key大于等于所有槽key，走最后一个槽的child
        off, length = internal_page._read_slot(internal_page.entry_count-1)
        entry_bytes = internal_page.data[off:off+length]
        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
        child_page_id = struct.unpack('I', entry_bytes[key_len:])[0]
        return child_page_id

    def search(self, transaction: 'Transaction', key: tuple) -> Optional[tuple]:
        """
        从 B+ 树中查找一个键，返回对应的 row_id。
        :param transaction: 事务对象。
        :param key: 要查找的键（元组格式）。
        :return: row_id (元组)，如果未找到则返回 None。
        """
        if self.root_page_id is None:
            return None # 树为空
        current_page_id = self.root_page_id
        while True:
            page = self._get_page(current_page_id)
            try:
                if isinstance(page, BTreeLeafPage):
                    result = page.search(key, self.key_col_types)
                    return result
                elif isinstance(page, BTreeInternalPage):
                    current_page_id = self._find_internal_child_page_id(page, key)
                else:
                    raise TypeError(f"未知的B+树页面类型: {type(page)}")
            finally:
                self.bp.unpin_page(page.page_id, is_dirty=False) 

    def _find_path_to_leaf(self, transaction, key: tuple):
        """
        查找插入key的叶子页，并返回路径（页面id列表）。
        :return: [root_id, ..., leaf_id]
        """
        path = []
        current_page_id = self.root_page_id
        while True:
            path.append(current_page_id)
            page = self._get_page(current_page_id)
            try:
                if isinstance(page, BTreeLeafPage):
                    return path
                elif isinstance(page, BTreeInternalPage):
                    current_page_id = self._find_internal_child_page_id(page, key)
                else:
                    raise TypeError(f"未知的B+树页面类型: {type(page)}")
            finally:
                self.bp.unpin_page(page.page_id, is_dirty=False)

    def print_tree(self):
        """递归打印整棵B+树结构，便于调试分裂和插入。"""
        def _print(page_id, level):
            page = self._get_page(page_id)
            indent = '  ' * level
            if isinstance(page, BTreeLeafPage):
                print(f"{indent}LeafPage id={page.page_id} entries={page.entry_count} next={page.next_leaf_page_id}")
                for i in range(page.entry_count):
                    off, length = page._read_slot(i)
                    entry_bytes = page.data[off:off+length]
                    key_len = page.key_col_types and page.key_col_types and page.key_col_types[0] and KeySerializer.get_key_length_from_bytes(entry_bytes, page.key_col_types) or 0
                    if key_len == 0 or key_len > len(entry_bytes):
                        slot_key = ()
                    else:
                        slot_key_bytes = entry_bytes[:key_len]
                        slot_key = KeySerializer.deserialize_key(slot_key_bytes, page.key_col_types)
                    print(f"{indent}  key={slot_key}")
            elif isinstance(page, BTreeInternalPage):
                print(f"{indent}InternalPage id={page.page_id} entries={page.entry_count} leftmost={page.leftmost_child_page_id}")
                # 打印leftmost_child
                print(f"{indent}  leftmost_child={page.leftmost_child_page_id}")
                if page.leftmost_child_page_id != None and page.leftmost_child_page_id != 0xFFFFFFFF:
                    _print(page.leftmost_child_page_id, level+1)
                for i in range(page.entry_count):
                    off, length = page._read_slot(i)
                    entry_bytes = page.data[off:off+length]
                    key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, page.key_col_types)
                    slot_key = KeySerializer.deserialize_key(entry_bytes[:key_len], page.key_col_types)
                    child_id = int.from_bytes(entry_bytes[key_len:key_len+4], 'little')
                    print(f"{indent}  key={slot_key} -> child={child_id}")
                    _print(child_id, level+1)
            else:
                print(f"{indent}UnknownPage id={page_id}")
            self.bp.unpin_page(page_id, is_dirty=False)
        if self.root_page_id is None:
            print("[BPTREE] 空树")
        else:
            print("[BPTREE] 当前树结构:")
            _print(self.root_page_id, 0)

    def insert(self, transaction: 'Transaction', key: tuple, row_id: tuple, lsn: int):
        """
        向 B+ 树中插入一个 (key, row_id) 对。
        :param transaction: 事务对象。
        :param key: 插入的key。
        :param row_id: 插入的row_id。
        :param lsn: 日志序列号。
        """
        # 情况1：树为空
        if self.root_page_id is None:
            page = self.bp.new_page(page_cls=BTreeLeafPage, key_col_types=self.key_col_types) # new_page 返回时已 pin
            leaf = page
            leaf.page_type = BTREE_LEAF
            leaf._save_header()  # 确保类型写入页头
            self.root_page_id = leaf.page_id
            self.cm.update_index_root_page(transaction, self.table_name, self.index_name, self.root_page_id)
            leaf.insert(transaction, key, row_id, self.key_col_types, is_unique=self.is_unique, lsn=lsn)
            self.bp.unpin_page(leaf.page_id, is_dirty=True)
            return
        # 情况2：树不为空
        path_to_leaf = self._find_path_to_leaf(transaction, key)
        leaf_page_id = path_to_leaf[-1]
        leaf_page = self._get_page(leaf_page_id)
        ok = leaf_page.insert(transaction, key, row_id, self.key_col_types, is_unique=self.is_unique, lsn=lsn)
        self.bp.unpin_page(leaf_page_id, is_dirty=True)
        if ok:
            return
        self._handle_split(transaction, path_to_leaf, key, row_id, lsn)
       

    def _handle_split(self, transaction: 'Transaction', path_to_leaf, key, row_id, lsn: int):
        """
        处理插入导致的分裂，递归向上分裂和根提升。
        :param path_to_leaf: 页面id列表，从根到叶
        :param key: 插入的key
        :param row_id: 插入的row_id
        :param transaction: 事务对象
        :param lsn: 日志序列号
        """
        # 1. 先在叶子页分裂
        leaf_page_id = path_to_leaf[-1]
        leaf_page = self._get_page(leaf_page_id)
        # 分配新叶子页
        new_right_leaf = self.bp.new_page(page_cls=BTreeLeafPage, key_col_types=self.key_col_types)
        min_key = leaf_page.split(transaction, new_right_leaf, lsn=lsn)

        # 插入新key到正确的叶子页
        if key >= min_key:
            new_right_leaf.insert(transaction, key, row_id, self.key_col_types, is_unique=self.is_unique, lsn=lsn)
            self.bp.unpin_page(new_right_leaf.page_id, is_dirty=True)
        else:
            leaf_page.insert(transaction, key, row_id, self.key_col_types, is_unique=self.is_unique, lsn=lsn)
            self.bp.unpin_page(leaf_page.page_id, is_dirty=True)

        for i in range(new_right_leaf.entry_count):
            off, length = new_right_leaf._read_slot(i)
            entry_bytes = new_right_leaf.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, new_right_leaf.key_col_types)
            slot_key = KeySerializer.deserialize_key(entry_bytes[:key_len], new_right_leaf.key_col_types)

        # 上推min_key到父节点
        up_key = min_key
        new_child_id = new_right_leaf.page_id
        
        # 2. 递归向上分裂
        for level in range(len(path_to_leaf)-2, -1, -1):
            parent_id = path_to_leaf[level]
            parent_page = self._get_page(parent_id)
            ok = parent_page.insert(transaction, up_key, new_child_id, self.key_col_types, lsn=lsn)
            self.bp.unpin_page(parent_id, is_dirty=True)
            if ok:
                return
            # 分配新内部页
            new_right = self.bp.new_page(page_cls=BTreeInternalPage, key_col_types=self.key_col_types)
            up_key, new_right = parent_page.split(transaction, new_right.page_id, lsn=lsn)
            new_child_id = new_right.page_id
            self.bp.unpin_page(new_right.page_id, is_dirty=True)
        # 3. 根节点分裂，创建新根
        new_root_page = self.bp.new_page(page_cls=BTreeInternalPage, key_col_types=self.key_col_types)
        new_root = new_root_page
        new_root.page_type = BTREE_INTERNAL
        new_root.set_leftmost_child(self.root_page_id)
        new_root.insert(transaction, up_key, new_child_id, self.key_col_types, lsn=lsn)
        self.root_page_id = new_root.page_id
        self.cm.update_index_root_page(self.table_name, self.index_name, self.root_page_id)
        self.bp.unpin_page(new_root.page_id, is_dirty=True)

    def delete(self, transaction: 'Transaction', key: tuple, row_id: tuple, lsn: int):
        """
        从B+树中删除一个(key, row_id)对。
        :param transaction: 事务对象。
        :param key: 删除的key。
        :param row_id: 删除的row_id。
        :param lsn: 日志序列号。
        """
        if self.root_page_id is None:
            return False
        
        # 查找叶子页路径
        path_to_leaf = self._find_path_to_leaf(transaction, key)
        leaf_page_id = path_to_leaf[-1]
        leaf_page = self._get_page(leaf_page_id)
        deleted, is_underflow = leaf_page.delete(transaction, key, row_id, self.key_col_types, lsn=lsn)
        self.bp.unpin_page(leaf_page_id, is_dirty=True)
        
        if not deleted:
            return False
        # 如果叶子页下溢，触发再平衡
        if is_underflow:
            self._handle_rebalance(transaction, path_to_leaf, lsn)
        return True

    def _find_siblings(self, path):
        """
        返回 (parent_page, current_page, left_sibling, right_sibling)
        left_sibling/right_sibling为None表示没有。
        """
        if len(path) < 2:
            return None, None, None, None
        parent_id = path[-2]
        current_id = path[-1]
        parent = self._get_page(parent_id)
        current = self._get_page(current_id)
        left = right = None
        found = False
        # 检查leftmost_child
        if parent.get_leftmost_child() == current_id:
            found = True
            # left = None
            if parent.entry_count > 0:
                # right兄弟是第一个槽位指向的页面
                off, length = parent._read_slot(0)
                entry_bytes = parent.data[off:off+length]
                key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                right_id = int.from_bytes(entry_bytes[key_len:key_len+4], 'little')
                right = self._get_page(right_id)
        else:
            for i in range(parent.entry_count):
                off, length = parent._read_slot(i)
                entry_bytes = parent.data[off:off+length]
                key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                child_id = int.from_bytes(entry_bytes[key_len:key_len+4], 'little')
                if child_id == current_id:
                    found = True
                    # left兄弟
                    if i > 0:
                        off_l, len_l = parent._read_slot(i-1)
                        entry_bytes_l = parent.data[off_l:off_l+len_l]
                        key_len_l = KeySerializer.get_key_length_from_bytes(entry_bytes_l, self.key_col_types)
                        left_id = int.from_bytes(entry_bytes_l[key_len_l:key_len_l+4], 'little')
                        left = self._get_page(left_id)
                    else:
                        # leftmost_child是兄弟
                        left_id = parent.get_leftmost_child()
                        if left_id != current_id:
                            left = self._get_page(left_id)
                    # right兄弟
                    if i < parent.entry_count-1:
                        off_r, len_r = parent._read_slot(i+1)
                        entry_bytes_r = parent.data[off_r:off_r+len_r]
                        key_len_r = KeySerializer.get_key_length_from_bytes(entry_bytes_r, self.key_col_types)
                        right_id = int.from_bytes(entry_bytes_r[key_len_r:key_len_r+4], 'little')
                        right = self._get_page(right_id)
                    break
        if not found:
            raise ValueError("未在父节点找到指向当前节点的指针")
        return parent, current, left, right

    def _handle_rebalance(self, transaction: 'Transaction', path, lsn: int):
        """
        处理删除导致的下溢和再平衡（重分配/合并），支持递归和根节点下溢。
        :param path: 页面id列表，从根到叶
        :param transaction: 事务对象
        :param lsn: 日志序列号
        """
        parent, current, left, right = self._find_siblings(path)
        try:
            if parent is None:
                # 根节点下溢
                if isinstance(current, BTreeInternalPage) and current.entry_count == 0:
                    new_root_id = current.get_leftmost_child()
                    if new_root_id != NULL_PAGE_ID:
                        self.root_page_id = new_root_id
                        self.cm.update_index_root_page(self.table_name, self.index_name, self.root_page_id)
                return
            # 1. 尝试重分配
            if self._attempt_redistribution(transaction, parent, current, left, right, lsn):
                return
            # 2. 合并
            self._perform_merge(transaction, parent, current, left, right, lsn)
            # 3. 合并可能导致父节点下溢，递归检查
            if hasattr(parent, 'entry_count') and parent.entry_count < parent.min_size:
                self._handle_rebalance(transaction, path[:-1], lsn)
        finally:
            if parent: self.bp.unpin_page(parent.page_id, is_dirty=True)
            if current: self.bp.unpin_page(current.page_id, is_dirty=True)
            if left: self.bp.unpin_page(left.page_id, is_dirty=True)
            if right: self.bp.unpin_page(right.page_id, is_dirty=True)

    def _attempt_redistribution(self, transaction: 'Transaction', parent, current, left, right, lsn: int):
        try:
            # 叶子节点借位
            if isinstance(current, BTreeLeafPage):
                # 向左兄弟借位
                if left and left.entry_count > 1:
                    # 借左兄弟最后一个条目
                    off, length = left._read_slot(left.entry_count-1)
                    entry_bytes = left.data[off:off+length]
                    key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                    borrow_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
                    row_id_bytes = entry_bytes[key_len:key_len+8]
                    page_id, record_id = struct.unpack('II', row_id_bytes)
                    current.insert(transaction, borrow_key, (page_id, record_id), self.key_col_types, lsn=lsn)
                    left._delete_slot(left.entry_count-1)
                    left.entry_count -= 1
                    left._save_header()
                    current._save_header()
                    # 更新parent所有分隔符key为其右侧子节点的最小key
                    for i in range(parent.entry_count):
                        off_p, len_p = parent._read_slot(i)
                        entry_bytes_p = parent.data[off_p:off_p+len_p]
                        key_len_p = KeySerializer.get_key_length_from_bytes(entry_bytes_p, self.key_col_types)
                        child_id = struct.unpack('I', entry_bytes_p[key_len_p:])[0]
                        right_page = self._get_page(child_id)
                        if right_page.entry_count > 0:
                            off_r, len_r = right_page._read_slot(0)
                            entry_r = right_page.data[off_r:off_r+len_r]
                            key_len_r = KeySerializer.get_key_length_from_bytes(entry_r, self.key_col_types)
                            new_sep_key = KeySerializer.deserialize_key(entry_r[:key_len_r], self.key_col_types)
                            child_bytes = entry_bytes_p[key_len_p:]
                            new_entry = KeySerializer.serialize_key(new_sep_key) + child_bytes
                            parent.data[off_p:off_p+len(new_entry)] = new_entry
                    parent._save_header()
                    return True
                # 向右兄弟借位
                if right and right.entry_count > 1:
                    off, length = right._read_slot(0)
                    entry_bytes = right.data[off:off+length]
                    key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                    borrow_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
                    row_id_bytes = entry_bytes[key_len:key_len+8]
                    page_id, record_id = struct.unpack('II', row_id_bytes)
                    current.insert(transaction, borrow_key, (page_id, record_id), self.key_col_types, lsn=lsn)
                    right._delete_slot(0)
                    right.entry_count -= 1
                    right._save_header()
                    current._save_header()
                    # 更新parent所有分隔符key为其右侧子节点的最小key
                    for i in range(parent.entry_count):
                        off_p, len_p = parent._read_slot(i)
                        entry_bytes_p = parent.data[off_p:off_p+len_p]
                        key_len_p = KeySerializer.get_key_length_from_bytes(entry_bytes_p, self.key_col_types)
                        child_id = struct.unpack('I', entry_bytes_p[key_len_p:])[0]
                        right_page = self._get_page(child_id)
                        if right_page.entry_count > 0:
                            off_r, len_r = right_page._read_slot(0)
                            entry_r = right_page.data[off_r:off_r+len_r]
                            key_len_r = KeySerializer.get_key_length_from_bytes(entry_r, self.key_col_types)
                            new_sep_key = KeySerializer.deserialize_key(entry_r[:key_len_r], self.key_col_types)
                            child_bytes = entry_bytes_p[key_len_p:]
                            new_entry = KeySerializer.serialize_key(new_sep_key) + child_bytes
                            parent.data[off_p:off_p+len(new_entry)] = new_entry
                    parent._save_header()
                    return True
                return False
            # 内部节点借位
            if isinstance(current, BTreeInternalPage):
                if left and left.entry_count > 0:
                    # 1. 父节点分隔键降级到current最前面
                    off_sep, len_sep = parent._read_slot(0) # 第一个槽位
                    sep_entry = parent.data[off_sep:off_sep+len_sep]
                    sep_key_len = KeySerializer.get_key_length_from_bytes(sep_entry, self.key_col_types)
                    sep_key = KeySerializer.deserialize_key(sep_entry[:sep_key_len], self.key_col_types)
                    # 2. L的最后一个key和指针
                    off_l, len_l = left._read_slot(left.entry_count-1)
                    entry_l = left.data[off_l:off_l+len_l]
                    key_len_l = KeySerializer.get_key_length_from_bytes(entry_l, self.key_col_types)
                    l_key = KeySerializer.deserialize_key(entry_l[:key_len_l], self.key_col_types)
                    l_child = int.from_bytes(entry_l[key_len_l:key_len_l+4], 'little')
                    # 3. current的leftmost_child_page_id需要被更新为L的最后一个指针
                    old_leftmost = current.get_leftmost_child()
                    current.set_leftmost_child(l_child)
                    # 4. sep_key降级插入current最前面，指向原current的old_leftmost
                    current.insert(transaction, sep_key, old_leftmost, self.key_col_types, lsn=lsn)
                    # 5. L的最后一个key升级到父节点
                    parent_off, parent_len = parent._read_slot(0) # 第一个槽位
                    entry_bytes_parent = parent.data[parent_off:parent_off+parent_len]
                    child_bytes = entry_bytes_parent[sep_key_len:]
                    new_entry = KeySerializer.serialize_key(l_key) + child_bytes
                    parent.data[parent_off:parent_off+len(new_entry)] = new_entry
                    parent._save_header()
                    # 6. 删除L的最后一个槽
                    left._delete_slot(left.entry_count-1)
                    left.entry_count -= 1
                    left._save_header()
                    self.bp.unpin_page(left.page_id, is_dirty=True)
                    self.bp.unpin_page(parent.page_id, is_dirty=True)
                    self.bp.unpin_page(current.page_id, is_dirty=True)
                    return True
                if right and right.entry_count > 0:
                    # 1. 父节点分隔键降级到current最后
                    off_sep, len_sep = parent._read_slot(0) # 第一个槽位
                    sep_entry = parent.data[off_sep:off_sep+len_sep]
                    sep_key_len = KeySerializer.get_key_length_from_bytes(sep_entry, self.key_col_types)
                    sep_key = KeySerializer.deserialize_key(sep_entry[:sep_key_len], self.key_col_types)
                    # 2. R的第一个key和指针
                    off_r, len_r = right._read_slot(0)
                    entry_r = right.data[off_r:off_r+len_r]
                    key_len_r = KeySerializer.get_key_length_from_bytes(entry_r, self.key_col_types)
                    r_key = KeySerializer.deserialize_key(entry_r[:key_len_r], self.key_col_types)
                    r_child = int.from_bytes(entry_r[key_len_r:key_len_r+4], 'little')
                    # 3. current最后一个指针
                    # 需要找到current最后一个key的child_page_id
                    # 但B+树内部节点的child_page_id都在key后面，leftmost_child_page_id单独存
                    # 所以sep_key降级插入current，指向right的leftmost_child
                    right_leftmost = right.get_leftmost_child()
                    current.insert(transaction, sep_key, right_leftmost, self.key_col_types, lsn=lsn)
                    # 4. R的第一个key升级到父节点
                    parent_off, parent_len = parent._read_slot(0) # 第一个槽位
                    entry_bytes_parent = parent.data[parent_off:parent_off+parent_len]
                    child_bytes = entry_bytes_parent[sep_key_len:]
                    new_entry = KeySerializer.serialize_key(r_key) + child_bytes
                    parent.data[parent_off:parent_off+len(new_entry)] = new_entry
                    parent._save_header()
                    # 5. right的leftmost_child_page_id更新为r_child
                    right.set_leftmost_child(r_child)
                    # 6. 删除R的第一个槽
                    right._delete_slot(0)
                    right.entry_count -= 1
                    right._save_header()
                    self.bp.unpin_page(right.page_id, is_dirty=True)
                    self.bp.unpin_page(parent.page_id, is_dirty=True)
                    self.bp.unpin_page(current.page_id, is_dirty=True)
                    return True
                return False
            return False
        finally:
            if parent: self.bp.unpin_page(parent.page_id, is_dirty=True)
            if current: self.bp.unpin_page(current.page_id, is_dirty=True)
            if left: self.bp.unpin_page(left.page_id, is_dirty=True)
            if right: self.bp.unpin_page(right.page_id, is_dirty=True)

    def _perform_merge(self, transaction: 'Transaction', parent, current, left, right, lsn: int):
        try:
            # 优先合并到左兄弟
            if left:
                if isinstance(current, BTreeLeafPage):
                    for i in range(current.entry_count):
                        off, length = current._read_slot(i)
                        entry_bytes = current.data[off:off+length]
                        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                        merge_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
                        row_id_bytes = entry_bytes[key_len:key_len+8]
                        page_id, record_id = struct.unpack('II', row_id_bytes)
                        left.insert(transaction, merge_key, (page_id, record_id), self.key_col_types, lsn=lsn)
                    left._save_header()
                    # 精确删除父节点分隔键
                    idx = self._find_merge_sep_index(parent, left, current)
                    if idx is not None:
                        parent.delete_entry_by_index(idx)
                    self.bp.unpin_page(left.page_id, is_dirty=True)
                    self.bp.unpin_page(parent.page_id, is_dirty=True)
                    self.bp.unpin_page(current.page_id, is_dirty=True)
                    return
                elif isinstance(current, BTreeInternalPage):
                    if left:
                        idx = self._find_merge_sep_index(parent, left, current)
                        if idx is not None:
                            off_sep, len_sep = parent._read_slot(idx)
                            sep_entry = parent.data[off_sep:off_sep+len_sep]
                            sep_key_len = KeySerializer.get_key_length_from_bytes(sep_entry, self.key_col_types)
                            sep_key = KeySerializer.deserialize_key(sep_entry[:sep_key_len], self.key_col_types)
                            left.insert(transaction, sep_key, current.get_leftmost_child(), self.key_col_types, lsn=lsn)
                    for i in range(current.entry_count):
                        off, length = current._read_slot(i)
                        entry_bytes = current.data[off:off+length]
                        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                        merge_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
                        child_id = int.from_bytes(entry_bytes[key_len:key_len+4], 'little')
                        left.insert(transaction, merge_key, child_id, self.key_col_types, lsn=lsn)
                    left._save_header()
                    if idx is not None:
                        parent.delete_entry_by_index(idx)
                    self.bp.unpin_page(left.page_id, is_dirty=True)
                    self.bp.unpin_page(parent.page_id, is_dirty=True)
                    self.bp.unpin_page(current.page_id, is_dirty=True)
                    return
            if right:
                if isinstance(current, BTreeLeafPage):
                    for i in range(current.entry_count):
                        off, length = current._read_slot(i)
                        entry_bytes = current.data[off:off+length]
                        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                        merge_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
                        row_id_bytes = entry_bytes[key_len:key_len+8]
                        page_id, record_id = struct.unpack('II', row_id_bytes)
                        right.insert(transaction, merge_key, (page_id, record_id), self.key_col_types, lsn=lsn)
                    right._save_header()
                    idx = self._find_merge_sep_index(parent, current, right)
                    if idx is not None:
                        parent.delete_entry_by_index(idx)
                    self.bp.unpin_page(right.page_id, is_dirty=True)
                    self.bp.unpin_page(parent.page_id, is_dirty=True)
                    self.bp.unpin_page(current.page_id, is_dirty=True)
                    return
                elif isinstance(current, BTreeInternalPage):
                    idx = self._find_merge_sep_index(parent, current, right)
                    if idx is not None:
                        off_sep, len_sep = parent._read_slot(idx)
                        sep_entry = parent.data[off_sep:off_sep+len_sep]
                        sep_key_len = KeySerializer.get_key_length_from_bytes(sep_entry, self.key_col_types)
                        sep_key = KeySerializer.deserialize_key(sep_entry[:sep_key_len], self.key_col_types)
                        right.set_leftmost_child(current.get_leftmost_child())
                        right.insert(transaction, sep_key, right.get_leftmost_child(), self.key_col_types, lsn=lsn)
                    for i in range(current.entry_count):
                        off, length = current._read_slot(i)
                        entry_bytes = current.data[off:off+length]
                        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
                        merge_key = KeySerializer.deserialize_key(entry_bytes[:key_len], self.key_col_types)
                        child_id = int.from_bytes(entry_bytes[key_len:key_len+4], 'little')
                        right.insert(transaction, merge_key, child_id, self.key_col_types, lsn=lsn)
                    right._save_header()
                    if idx is not None:
                        parent.delete_entry_by_index(idx)
                    self.bp.unpin_page(right.page_id, is_dirty=True)
                    self.bp.unpin_page(parent.page_id, is_dirty=True)
                    self.bp.unpin_page(current.page_id, is_dirty=True)
                    return
            return
        finally:
            if parent: self.bp.unpin_page(parent.page_id, is_dirty=True)
            if current: self.bp.unpin_page(current.page_id, is_dirty=True)
            if left: self.bp.unpin_page(left.page_id, is_dirty=True)
            if right: self.bp.unpin_page(right.page_id, is_dirty=True)

    def _find_merge_sep_index(self, parent, left, right):
        """
        在parent中找到分隔left和right的槽位索引。
        """
        for i in range(parent.entry_count):
            off, length = parent._read_slot(i)
            entry_bytes = parent.data[off:off+length]
            key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, self.key_col_types)
            child_id = int.from_bytes(entry_bytes[key_len:key_len+4], 'little')
            if left.page_id == parent.get_leftmost_child() and child_id == right.page_id:
                return 0
            if i > 0:
                off_l, len_l = parent._read_slot(i-1)
                entry_bytes_l = parent.data[off_l:off_l+len_l]
                key_len_l = KeySerializer.get_key_length_from_bytes(entry_bytes_l, self.key_col_types)
                left_id = int.from_bytes(entry_bytes_l[key_len_l:key_len_l+4], 'little')
                if left.page_id == left_id and right.page_id == child_id:
                    return i
        return None 