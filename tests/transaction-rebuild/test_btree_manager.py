import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import pytest
from src.engine.storage.btree_manager import BTreeManager
from src.engine.catalog_manager import CatalogManager, TableInfo, IndexInfo, ColumnInfo
from src.storage.btreepage import BTreeLeafPage, BTreeInternalPage
from src.storage import BufferPool
from src.storage import TablespaceManager
from src.engine.transaction.transaction import Transaction, IsolationLevel
from src.engine.transaction.log_manager import LogManager
from src.storage.btreepage import KeySerializer

# --- 最小 RealStorageEngine mock 用于 LogManager 构造 ---
class DummyStorageEngine:
    def __init__(self, buffer_pools=None):
        self.buffer_pools = buffer_pools or {}
    def get_page_for_recovery(self, resource_id):
        return None

@pytest.fixture
def dummy_transaction():
    return Transaction(1, IsolationLevel.SERIALIZABLE)

@pytest.fixture
def dummy_catalog_manager(tmp_path):
    cm = CatalogManager()
    table_name = 'test_table'
    columns = [ColumnInfo('col_A', 'int')]
    cm.tables[table_name] = TableInfo(
        table_name=table_name,
        columns=columns,
        file_name=f'{table_name}.db',
        root_page_id=1,
        last_page_id=1,
        indexes={
            'index_A': IndexInfo('index_A', 'index_A.db', 10, ['col_A'], [1]),
        }
    )
    return cm

@pytest.fixture
def dummy_buffer_pool(tmp_path, dummy_catalog_manager):
    # 用临时文件做 tablespace
    table_file = tmp_path / 'test_table.db'
    index_file = tmp_path / 'index_A.db'
    # 创建物理文件
    TablespaceManager(str(table_file))
    TablespaceManager(str(index_file))
    ts = TablespaceManager(str(index_file))
    # 初始化第一页为合法 BTreeLeafPage，防止 struct.unpack 报错
    page = BTreeLeafPage(1, page_size=4096, key_col_types=[1])
    ts.write_page(1, page.data)
    # 最小 StorageEngine mock
    dummy_storage_engine = DummyStorageEngine()
    log = LogManager(str(tmp_path / 'test.log'), dummy_storage_engine)
    bp = BufferPool(ts, buffer_size=8, log_manager=log)
    return bp

def make_btree_manager_with_real(dummy_catalog_manager, dummy_buffer_pool):
    # 真实 BTreeManager，BufferPool
    btm = BTreeManager(dummy_buffer_pool, dummy_catalog_manager, 'test_table', 'index_A', [1])
    btm.root_page_id = 1
    return btm, dummy_buffer_pool

def test_btree_insert_simple_passes_context(dummy_transaction, dummy_catalog_manager, dummy_buffer_pool):
    btm, bp = make_btree_manager_with_real(dummy_catalog_manager, dummy_buffer_pool)
    # 插入一条数据
    key = (1,)
    row_id = (1, 1)
    lsn = 101
    result = btm.insert(dummy_transaction, key, row_id, lsn=lsn)
    # 断言插入成功
    leaf_page = bp.get_page(1, page_cls=BTreeLeafPage, key_col_types=[1])
    assert leaf_page.entry_count == 1
    # 检查插入内容
    found = leaf_page.search(key, [1])
    assert found == row_id
    # 检查 LSN
    assert leaf_page.get_page_lsn() == lsn
    bp.unpin_page(1, is_dirty=True)

# 其余测试用例（如 split/delete）可按此模式扩展，直接断言真实页面内容。 

def test_btree_insert_triggers_split(dummy_transaction, dummy_catalog_manager, dummy_buffer_pool):
    btm, bp = make_btree_manager_with_real(dummy_catalog_manager, dummy_buffer_pool)
    # 插入足够多的数据，触发叶子页分裂
    lsn = 202
    # 估算每页能放多少entry，B+树页默认4096字节，entry估算32字节，最多约120条，保险起见插入20条
    N = 20
    for i in range(1, N+1):
        btm.insert(dummy_transaction, (i,), (i, i), lsn=lsn)
    # 检查分裂：应有两个叶子页，且父节点页存在
    # 找到所有叶子页
    leaf1 = bp.get_page(1, page_cls=BTreeLeafPage, key_col_types=[1])
    leaf2 = None
    if leaf1.next_leaf_page_id != 0xFFFFFFFF:
        leaf2 = bp.get_page(leaf1.next_leaf_page_id, page_cls=BTreeLeafPage, key_col_types=[1])
    # 父节点页
    parent_id = None
    for page_id in bp.cache:
        if page_id != 1 and (leaf2 is None or page_id != leaf2.page_id):
            parent_id = page_id
            break
    parent = bp.get_page(parent_id, page_cls=BTreeInternalPage, key_col_types=[1]) if parent_id else None
    # 断言所有相关页面的 LSN
    assert leaf1.get_page_lsn() == lsn
    if leaf2:
        assert leaf2.get_page_lsn() == lsn
    if parent:
        assert parent.get_page_lsn() == lsn
    # 检查数据分布
    all_keys = set()
    for leaf in [leaf1, leaf2]:
        if leaf:
            for i in range(leaf.entry_count):
                off, length = leaf._read_slot(i)
                entry_bytes = leaf.data[off:off+length]
                key_len = leaf.key_col_types and KeySerializer.get_key_length_from_bytes(entry_bytes, leaf.key_col_types) or 0
                if key_len > 0:
                    slot_key = KeySerializer.deserialize_key(entry_bytes[:key_len], leaf.key_col_types)
                    all_keys.add(slot_key[0])
    assert all_keys == set(range(1, N+1))
    # 检查父节点分隔键
    if parent:
        assert parent.entry_count >= 1
    bp.unpin_page(1, is_dirty=True)
    if leaf2:
        bp.unpin_page(leaf2.page_id, is_dirty=True)
    if parent:
        bp.unpin_page(parent.page_id, is_dirty=True)

def test_btree_delete_simple_passes_context(dummy_transaction, dummy_catalog_manager, dummy_buffer_pool):
    btm, bp = make_btree_manager_with_real(dummy_catalog_manager, dummy_buffer_pool)
    # 插入三条数据
    for i in range(1, 4):
        btm.insert(dummy_transaction, (i,), (i, i), lsn=888)
    # 删除 key=2
    lsn = 999
    btm.delete(dummy_transaction, (2,), (2, 2), lsn=lsn)
    leaf = bp.get_page(1, page_cls=BTreeLeafPage, key_col_types=[1])
    assert leaf.get_page_lsn() == lsn
    assert leaf.search((2,), [1]) is None
    # 其它数据还在
    assert leaf.search((1,), [1]) == (1, 1)
    assert leaf.search((3,), [1]) == (3, 3)
    bp.unpin_page(1, is_dirty=True)

def test_btree_delete_triggers_merge(dummy_transaction, dummy_catalog_manager, dummy_buffer_pool):
    btm, bp = make_btree_manager_with_real(dummy_catalog_manager, dummy_buffer_pool)
    # 插入足够多数据，确保分裂出两个叶子页
    N = 20
    for i in range(1, N+1):
        btm.insert(dummy_transaction, (i,), (i, i), lsn=100)
    # 删除所有数据，只剩一条，确保触发合并
    lsn = 555
    for i in range(2, N+1):
        btm.delete(dummy_transaction, (i,), (i, i), lsn=lsn)
    # 现在只剩 key=1
    leaf1 = bp.get_page(1, page_cls=BTreeLeafPage, key_col_types=[1])
    # 检查 leaf1 LSN
    assert leaf1.get_page_lsn() == lsn
    # 检查只有 key=1
    keys = []
    for i in range(leaf1.entry_count):
        off, length = leaf1._read_slot(i)
        entry_bytes = leaf1.data[off:off+length]
        key_len = leaf1.key_col_types and KeySerializer.get_key_length_from_bytes(entry_bytes, leaf1.key_col_types) or 0
        if key_len > 0:
            slot_key = KeySerializer.deserialize_key(entry_bytes[:key_len], leaf1.key_col_types)
            keys.append(slot_key[0])
    assert keys == [1]
    # 检查父节点是否已移除分隔键（entry_count==0 或 1）
    parent_id = None
    for page_id in bp.cache:
        if page_id != 1:
            parent_id = page_id
            break
    if parent_id:
        parent = bp.get_page(parent_id, page_cls=BTreeInternalPage, key_col_types=[1])
        assert parent.get_page_lsn() == lsn
        assert parent.entry_count <= 1
        bp.unpin_page(parent.page_id, is_dirty=True)
    bp.unpin_page(1, is_dirty=True) 