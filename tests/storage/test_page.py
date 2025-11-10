import pytest
from src.storage.page import BasePage, HeapPage

def test_basepage_init_and_bytes():
    page = BasePage(page_id=1, page_size=128)
    assert page.page_id == 1
    assert page.page_size == 128
    b = page.to_bytes()
    assert isinstance(b, bytes)
    page.mark_dirty()
    assert page.is_dirty
    page.pin()
    assert page.pin_count == 1
    page.unpin()
    assert page.pin_count == 0

def test_heap_page_insert_update_delete():
    page = HeapPage(page_id=2, page_size=128)
    data = b'abcde'
    lsn = 123
    rid = page.insert_record(None, data, lsn)
    assert isinstance(rid, int)
    valid, row = page.get_record(rid, len(data))
    assert valid and row == data
    # update
    new_data = b'xyz12'
    ok = page.update_record(None, rid, new_data, lsn)
    assert ok
    valid, row = page.get_record(rid, len(new_data))
    assert valid and row == new_data
    # mark as deleted
    ok = page.mark_as_deleted(None, rid, len(new_data), lsn)
    assert ok
    valid, row = page.get_record(rid, len(new_data))
    assert not valid

def test_heap_page_is_full():
    page = HeapPage(page_id=3, page_size=64)
    data = b'x'*10
    lsn = 1
    for _ in range(2):
        page.insert_record(None, data, lsn)
    assert not page.is_full(len(data))
    for _ in range(10):
        try:
            page.insert_record(None, data, lsn)
        except ValueError:
            break
    assert page.is_full(len(data))

def test_heap_page_from_bytes_and_repr():
    page = HeapPage(page_id=4, page_size=128)
    data = b'hello'
    lsn = 1
    rid = page.insert_record(None, data, lsn)
    b = page.to_bytes()
    page2 = HeapPage.from_bytes(page_id=4, data=b, page_size=128)
    valid, row = page2.get_record(rid, len(data))
    assert valid and row == data
    s = repr(page2)
    assert "Page id=4" in s 