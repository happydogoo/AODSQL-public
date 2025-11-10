import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import tempfile
import pytest
from src.storage import BufferPool
from src.engine.transaction.log_manager import LogManager
from src.storage import TablespaceManager
from src.storage import HeapPage

class DummyStorageEngine:
    pass

@pytest.fixture
def temp_db_file():
    with tempfile.NamedTemporaryFile(delete=False) as f:
        fname = f.name
    # 主动写入4字节文件头，防止struct.error
    with open(fname, 'wb') as f:
        f.write(b'\xff\xff\xff\xff')
    yield fname
    # 不再删除文件，交由tablespace_manager fixture处理

@pytest.fixture
def tablespace_manager(temp_db_file):
    mgr = TablespaceManager(temp_db_file, page_size=4096)
    yield mgr
    mgr.close()
    try:
        mgr.delete_file()
    except PermissionError:
        pass

@pytest.fixture
def log_manager(tmp_path):
    log_file = tmp_path / "test.log"
    mgr = LogManager(str(log_file), DummyStorageEngine())
    yield mgr
    mgr._log_file.close()
    if os.path.exists(log_file):
        os.remove(log_file)

@pytest.mark.parametrize("page_lsn, flushed_lsn, should_flush_log", [
    (150, 200, False),
    (150, 100, True),
])
def test_flush_page_wal_behavior(tablespace_manager, log_manager, page_lsn, flushed_lsn, should_flush_log):
    buffer_pool = BufferPool(tablespace_manager, buffer_size=2, log_manager=log_manager)
    page = buffer_pool.new_page(HeapPage)
    page.set_page_lsn(page_lsn)
    page.is_dirty = True
    buffer_pool.dirty_pages.add(page.page_id)
    buffer_pool.pin_count[page.page_id] = 0
    log_manager._flushed_lsn = flushed_lsn
    called = {}
    def fake_flush_to_lsn(lsn):
        called['lsn'] = lsn
        log_manager._flushed_lsn = lsn
    log_manager.flush_to_lsn = fake_flush_to_lsn
    write_called = {}
    orig_write_page = tablespace_manager.write_page
    def fake_write_page(page_id, data):
        write_called['called'] = (page_id, data)
        return orig_write_page(page_id, data)
    tablespace_manager.write_page = fake_write_page
    buffer_pool.flush_page(page.page_id)
    if should_flush_log:
        assert called['lsn'] == page_lsn
    else:
        assert 'lsn' not in called
    assert write_called['called'][0] == page.page_id 