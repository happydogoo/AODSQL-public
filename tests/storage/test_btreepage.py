import struct
import pytest
from src.storage.btreepage import (
    KeySerializer, BTreeLeafPage, BTreeInternalPage,
    KEY_TYPE_INT, KEY_TYPE_STR, KEY_TYPE_FLOAT
)

PAGE_SIZE = 512

def test_key_serializer_basic():
    # int
    b = KeySerializer.serialize_key(123)
    assert isinstance(b, bytes)
    k = KeySerializer.deserialize_key(b, [KEY_TYPE_INT])
    assert k == (123,)
    # float
    b = KeySerializer.serialize_key(3.14)
    k = KeySerializer.deserialize_key(b, [KEY_TYPE_FLOAT])
    assert abs(k[0] - 3.14) < 1e-6
    # str
    b = KeySerializer.serialize_key("abc")
    k = KeySerializer.deserialize_key(b, [KEY_TYPE_STR])
    assert k == ("abc",)
    # tuple
    b = KeySerializer.serialize_key((1, "x", 2.5))
    k = KeySerializer.deserialize_key(b, [KEY_TYPE_INT, KEY_TYPE_STR, KEY_TYPE_FLOAT])
    assert k == (1, "x", 2.5)
    # key_length
    assert KeySerializer.key_length((1, "x")) == len(KeySerializer.serialize_key((1, "x")))

def test_leaf_page_insert_search_delete():
    page = BTreeLeafPage(page_id=1, page_size=PAGE_SIZE, key_col_types=[KEY_TYPE_INT])
    for i in range(5):
        assert page.insert(None, (i,), (100+i, i), [KEY_TYPE_INT], lsn=0)
    for i in range(5):
        row_id = page.search((i,), [KEY_TYPE_INT])
        assert row_id == (100+i, i)
    # 删除
    assert page.delete(None, (2,), (102, 2), [KEY_TYPE_INT], lsn=0)[0]
    assert page.search((2,), [KEY_TYPE_INT]) is None

def test_leaf_page_split_and_compact():
    page = BTreeLeafPage(page_id=2, page_size=PAGE_SIZE, key_col_types=[KEY_TYPE_INT])
    inserted = 0
    while True:
        ok = page.insert(None, (inserted,), (200+inserted, inserted), [KEY_TYPE_INT], lsn=0)
        if not ok:
            break
        inserted += 1
    # 分裂
    class DummyPage:
        def __init__(self):
            self.page_id = 3
            self.entry_count = 0
            self.key_col_types = [KEY_TYPE_INT]
            self.next_leaf_page_id = None
            self.data = bytearray(PAGE_SIZE)
        def insert(self, *args, **kwargs):
            return True
    new_page = BTreeLeafPage(page_id=3, page_size=PAGE_SIZE, key_col_types=[KEY_TYPE_INT])
    min_key = page.split(None, new_page, lsn=0)
    assert new_page.entry_count > 0
    assert min_key is not None
    # 紧凑
    before = page.get_free_space()
    page._compact()
    after = page.get_free_space()
    assert after >= before

def test_leaf_page_composite_key():
    page = BTreeLeafPage(page_id=4, page_size=PAGE_SIZE, key_col_types=[KEY_TYPE_INT, KEY_TYPE_STR])
    keys = [(i, f"k{i}") for i in range(3)]
    for i, k in enumerate(keys):
        assert page.insert(None, k, (300+i, i), [KEY_TYPE_INT, KEY_TYPE_STR], lsn=0)
    for i, k in enumerate(keys):
        row_id = page.search(k, [KEY_TYPE_INT, KEY_TYPE_STR])
        assert row_id == (300+i, i)

def test_internal_page_insert_split_delete():
    page = BTreeInternalPage(page_id=10, page_size=PAGE_SIZE, key_col_types=[KEY_TYPE_INT])
    page.set_leftmost_child(999)
    for i in range(7):
        assert page.insert(None, (i,), 1000+i, [KEY_TYPE_INT], lsn=0)
    # 分裂后, page 中将包含键 (0, 1, 2), new_right 将包含 (4, 5, 6)。键 (3) 被上推。
    up_key, new_right = page.split(None, new_page_id=11, lsn=0)
    assert up_key == (3,)  # 验证上推的键是否正确
    assert new_right.entry_count > 0
    assert page.entry_count == 3 # 验证原页面的键数量
    # 删除一个分裂后仍然存在的键, 例如 (2,)
    assert page.delete(None, (2,), [KEY_TYPE_INT], lsn=0)
    # 验证键 (2,) 确实已被删除
    key_found = False
    for i in range(page.entry_count):
        off, length = page._read_slot(i)
        entry_bytes = page.data[off:off+length]
        key_len = KeySerializer.get_key_length_from_bytes(entry_bytes, [KEY_TYPE_INT])
        key = KeySerializer.deserialize_key(entry_bytes[:key_len], [KEY_TYPE_INT])
        if key == (2,):
            key_found = True
            break
    assert not key_found, "被删除的键 (2,) 仍然存在"
    # 验证一个不存在的键 (3,) 无法被删除
    assert not page.delete(None, (3,), [KEY_TYPE_INT], lsn=0)
    # leftmost_child_page_id 应该保持不变
    assert page.get_leftmost_child() == 999 