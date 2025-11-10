import sys
import os
import pytest
import struct
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from src.engine.storage.index_manager import IndexManager
from src.engine.catalog_manager import CatalogManager, TableInfo, IndexInfo, ColumnInfo
from src.storage.btreepage import BTreeLeafPage, KeySerializer
from src.storage import BufferPool
from src.storage import TablespaceManager
from src.engine.transaction.transaction import Transaction, IsolationLevel
from src.engine.transaction.log_manager import LogManager

@pytest.fixture
def dummy_transaction():
    return Transaction(1, IsolationLevel.SERIALIZABLE)

@pytest.fixture
def real_catalog_and_buffers(tmp_path):
    # 创建真实的表和两个索引
    cm = CatalogManager()
    table_name = 'test_table'
    columns = [ColumnInfo('col_A', 'int'), ColumnInfo('col_B', 'int')]
    index_A_file = tmp_path / 'index_A.db'
    index_B_file = tmp_path / 'index_B.db'
    # 初始化物理文件和第一页
    for idx_file in [index_A_file, index_B_file]:
        TablespaceManager(str(idx_file))
    ts_A = TablespaceManager(str(index_A_file))
    ts_B = TablespaceManager(str(index_B_file))
    # 初始化第一页为合法BTreeLeafPage
    page_A = BTreeLeafPage(1, page_size=4096, key_col_types=[1])
    page_B = BTreeLeafPage(1, page_size=4096, key_col_types=[1])
    ts_A.write_page(1, page_A.data)
    ts_B.write_page(1, page_B.data)
    # 日志和缓冲池
    dummy_storage_engine = type('DummySE', (), {'get_page_for_recovery': lambda self, rid: None})()
    log = LogManager(str(tmp_path / 'test.log'), dummy_storage_engine)
    bp_A = BufferPool(ts_A, buffer_size=8, log_manager=log)
    bp_B = BufferPool(ts_B, buffer_size=8, log_manager=log)
    # 注册到CatalogManager
    cm.tables[table_name] = TableInfo(
        table_name=table_name,
        columns=columns,
        file_name=f'{table_name}.db',
        root_page_id=1,
        last_page_id=1,
        indexes={
            'index_A': IndexInfo('index_A', str(index_A_file), 1, ['col_A'], [1]),
            'index_B': IndexInfo('index_B', str(index_B_file), 1, ['col_B'], [1]),
        }
    )
    return cm, {'index_A': bp_A, 'index_B': bp_B}

def get_leaf_entries(bp, key_col_types):
    leaf = bp.get_page(1, page_cls=BTreeLeafPage, key_col_types=key_col_types)
    entries = []
    for i in range(leaf.entry_count):
        off, length = leaf._read_slot(i)
        entry_bytes = leaf.data[off:off+length]
        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, key_col_types)
        key = KeySerializer.deserialize_key(entry_bytes[:key_len], key_col_types)
        row_id = struct.unpack('<II', entry_bytes[key_len:key_len+8])
        entries.append((key, row_id))
    return entries

def test_insert_entry_on_multiple_indexes(dummy_transaction, real_catalog_and_buffers):
    cm, index_bps = real_catalog_and_buffers
    table_info = cm.get_table('test_table')
    im = IndexManager(table_info, None, index_bps, cm)
    lsn_map = {'index_A': 401, 'index_B': 402}
    # 插入一条 (1, 100)
    im.insert_entry(dummy_transaction, (1, 100), (1, 0), lsn_map)
    # 检查index_A
    entries_A = get_leaf_entries(index_bps['index_A'], [1])
    assert ((1,), (1, 0)) in entries_A
    # 检查index_B
    entries_B = get_leaf_entries(index_bps['index_B'], [1])
    assert ((100,), (1, 0)) in entries_B
    index_bps['index_A'].unpin_page(1, is_dirty=True)
    index_bps['index_B'].unpin_page(1, is_dirty=True)

def test_update_entries_when_key_changes(dummy_transaction, real_catalog_and_buffers):
    cm, index_bps = real_catalog_and_buffers
    table_info = cm.get_table('test_table')
    im = IndexManager(table_info, None, index_bps, cm)
    # 先插入 (1, 100)
    lsn_map = {'index_A': 501, 'index_B': 600}
    im.insert_entry(dummy_transaction, (1, 100), (1, 0), lsn_map)
    # 更新 col_A 从1->2
    lsn_map2 = {'index_A_delete': 501, 'index_A_insert': 502, 'index_B': 600, 'index_A': 502}
    im.update_entries(dummy_transaction, (1, 100), (2, 100), (1, 0), lsn_map2)
    # index_A 只应有 (2,)
    entries_A = get_leaf_entries(index_bps['index_A'], [1])
    assert ((2,), (1, 0)) in entries_A
    assert ((1,), (1, 0)) not in entries_A
    # index_B 不变
    entries_B = get_leaf_entries(index_bps['index_B'], [1])
    assert ((100,), (1, 0)) in entries_B
    index_bps['index_A'].unpin_page(1, is_dirty=True)
    index_bps['index_B'].unpin_page(1, is_dirty=True)

def test_update_entries_when_key_is_unchanged(dummy_transaction, real_catalog_and_buffers):
    cm, index_bps = real_catalog_and_buffers
    table_info = cm.get_table('test_table')
    im = IndexManager(table_info, None, index_bps, cm)
    # 先插入 (1, 100)
    lsn_map = {'index_A': 701, 'index_B': 702}
    im.insert_entry(dummy_transaction, (1, 100), (1, 0), lsn_map)
    # 更新 col_B 从100->200
    lsn_map2 = {'index_A': 701, 'index_B': 702}
    im.update_entries(dummy_transaction, (1, 100), (1, 200), (1, 0), lsn_map2)
    # index_A 仍然只有 (1,)
    entries_A = get_leaf_entries(index_bps['index_A'], [1])
    assert ((1,), (1, 0)) in entries_A
    # index_B 变为 (200,)
    entries_B = get_leaf_entries(index_bps['index_B'], [1])
    assert ((200,), (1, 0)) in entries_B
    assert ((100,), (1, 0)) not in entries_B
    index_bps['index_A'].unpin_page(1, is_dirty=True)
    index_bps['index_B'].unpin_page(1, is_dirty=True) 