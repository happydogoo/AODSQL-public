import pytest
from src.storage.buffer import BufferPool
from src.storage.page import HeapPage

class DummyTablespaceManager:
    def __init__(self, page_size=128):
        self.page_size = page_size
        self.pages = {}
        self.next_id = 1
    def read_page(self, page_id):
        return self.pages.get(page_id, b'\x00'*self.page_size)
    def write_page(self, page_id, data):
        self.pages[page_id] = data
    def allocate_page(self):
        pid = self.next_id
        self.next_id += 1
        self.pages[pid] = b'\x00'*self.page_size
        return pid

class DummyLogManager:
    def get_flushed_lsn(self):
        return 0
    def flush_to_lsn(self, lsn):
        pass

def test_bufferpool_basic():
    tsm = DummyTablespaceManager(page_size=128)
    logm = DummyLogManager()
    bp = BufferPool(tsm, buffer_size=2, log_manager=logm)
    # new_page
    page = bp.new_page()
    assert isinstance(page, HeapPage)
    pid = page.page_id
    # get_page
    page2 = bp.get_page(pid)
    assert page2.page_id == pid
    # unpin
    bp.unpin_page(pid, is_dirty=True)
    assert pid in bp.dirty_pages
    # flush_page
    bp.flush_page(pid)
    assert pid not in bp.dirty_pages
    # free_page
    bp.free_page(pid)
    assert pid not in bp.cache

def test_bufferpool_lru_evict():
    tsm = DummyTablespaceManager(page_size=128)
    logm = DummyLogManager()
    bp = BufferPool(tsm, buffer_size=2, log_manager=logm)
    p1 = bp.new_page()
    p2 = bp.new_page()
    bp.unpin_page(p1.page_id, is_dirty=False)
    bp.unpin_page(p2.page_id, is_dirty=False)
    p3 = bp.new_page()
    # p1或p2应被淘汰
    assert len(bp.cache) == 2
    # pin_count=1
    assert bp.pin_count[p3.page_id] == 1

def test_bufferpool_flush_all():
    tsm = DummyTablespaceManager(page_size=128)
    logm = DummyLogManager()
    bp = BufferPool(tsm, buffer_size=2, log_manager=logm)
    p1 = bp.new_page()
    p2 = bp.new_page()
    bp.unpin_page(p1.page_id, is_dirty=True)
    bp.unpin_page(p2.page_id, is_dirty=True)
    bp.flush_all()
    assert not bp.dirty_pages

def test_bufferpool_delete_table_pages():
    tsm = DummyTablespaceManager(page_size=128)
    logm = DummyLogManager()
    bp = BufferPool(tsm, buffer_size=4, log_manager=logm)
    # 构造链表页
    class DummyPage(HeapPage):
        def __init__(self, page_id, next_page_id=0, page_size=128):
            super().__init__(page_id, page_size=page_size)
            self.next_page_id = next_page_id
    pids = [bp.new_page().page_id for _ in range(3)]
    for i in range(2):
        page = bp.cache[pids[i]]
        page.next_page_id = pids[i+1]
    page = bp.cache[pids[2]]
    page.next_page_id = 0
    bp.delete_table_pages(pids[0])
    for pid in pids:
        assert pid not in bp.cache 