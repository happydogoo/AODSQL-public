import os
import tempfile
import pytest
from src.storage.tablespace_manager import TablespaceManager

def test_tablespace_manager_allocate_read_write_free():
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        path = tf.name
    try:
        mgr = TablespaceManager(path, page_size=64)
        # allocate
        pid1 = mgr.allocate_page()
        assert pid1 == 1
        pid2 = mgr.allocate_page()
        assert pid2 == 2
        # write/read
        data = b'abc' + b'\x00'*61
        mgr.write_page(pid1, data)
        read = mgr.read_page(pid1)
        assert read[:3] == b'abc'
        # free_page
        mgr.free_page(pid1)
        # 再分配应复用pid1
        pid3 = mgr.allocate_page()
        assert pid3 == pid1
        # close
        mgr.close()
        # reopen
        mgr2 = TablespaceManager(path, page_size=64)
        assert mgr2.read_page(pid2)[:3] == b'\x00'*3
        mgr2.close()
    finally:
        os.remove(path)

def test_tablespace_manager_delete_file():
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        path = tf.name
    mgr = TablespaceManager(path, page_size=32)
    mgr.allocate_page()
    mgr.delete_file()
    assert not os.path.exists(path) 