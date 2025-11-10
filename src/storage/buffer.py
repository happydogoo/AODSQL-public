"""
缓冲池（BufferPool）模块。

职责：
- 提供页缓存（LRU）与钉住计数管理
- 脏页刷新与 WAL 顺序保证（基于 LogManager 的 LSN）
- 抽象出从 `TablespaceManager` 读取/写入页的细节

遵循项目规范：
- 文档字符串与类型注解
- 不改变原有逻辑
"""

from collections import OrderedDict
from .page import HeapPage
from .tablespace_manager import TablespaceManager
from typing import TYPE_CHECKING, Type, Any
# from engine.transaction.log_manager import LogManager
if TYPE_CHECKING:
    from src.engine.transaction.log_manager import LogManager
class BufferPool:
    """
    缓冲池，协调Page和TablespaceManager，管理内存页缓存。
    支持LRU淘汰、脏页管理、钉住计数。
    """
    def __init__(self, tablespace_manager: TablespaceManager, buffer_size: int, log_manager: 'LogManager') -> None:
        self.tablespace_manager = tablespace_manager
        self.buffer_size = buffer_size
        self.log_manager: 'LogManager' = log_manager
        self.cache = OrderedDict()  # page_id -> Page
        self.dirty_pages = set()    # page_id集合
        self.pin_count = {}         # page_id -> pin计数

    def get_page(self, page_id: int, page_cls: Type[HeapPage] = HeapPage, *args: Any, **kwargs: Any) -> HeapPage:
        """
        获取指定页，优先从缓存，否则从磁盘加载。
        页被自动钉住（pin_count+1）。
        支持传入页类型（如HeapPage、BTreeLeafPage、BTreeInternalPage等）。
        :param page_cls: 页类，默认为HeapPage
        :param args, kwargs: 传递给页类的其他参数
        """
        if page_id in self.cache:
            page = self.cache.pop(page_id)
            self.cache[page_id] = page  # LRU: move to end
        else:
            if len(self.cache) >= self.buffer_size:
                self._evict_page()
            data = self.tablespace_manager.read_page(page_id)
            page = page_cls(page_id, data=data, page_size=self.tablespace_manager.page_size, *args, **kwargs)
            self.cache[page_id] = page
        self.pin_count[page_id] = self.pin_count.get(page_id, 0) + 1
        return page

    def unpin_page(self, page_id: int, is_dirty: bool) -> None:
        """
        通知缓冲池页已使用完毕，减少pin计数，并标记脏页。
        """
        if page_id not in self.cache:
            return
        if is_dirty:
            self.dirty_pages.add(page_id)
            self.cache[page_id].is_dirty = True
        if self.pin_count.get(page_id, 0) > 0:
            self.pin_count[page_id] -= 1

    def flush_page(self, page_id: int) -> None:
        """
        强制将指定脏页写回磁盘。
        """
        if page_id in self.cache and self.cache[page_id].is_dirty:
            page = self.cache[page_id]
            page_lsn = page.get_page_lsn()
            flushed_lsn = self.log_manager.get_flushed_lsn()
            if page_lsn > flushed_lsn:
                self.log_manager.flush_to_lsn(page_lsn)
            self.tablespace_manager.write_page(page_id, page.to_bytes())
            page.is_dirty = False
            if page_id in self.dirty_pages:
                self.dirty_pages.remove(page_id)

    def new_page(self, page_cls: Type[HeapPage] = HeapPage, *args: Any, **kwargs: Any) -> HeapPage:
        """
        分配新页并加载到缓存。支持传入页类型（如HeapPage、BTreeLeafPage、BTreeInternalPage等）。
        :param page_cls: 页类，默认为HeapPage
        :param args, kwargs: 传递给页类的其他参数
        """
        page_id = self.tablespace_manager.allocate_page()
        page = page_cls(page_id, page_size=self.tablespace_manager.page_size, *args, **kwargs)
        if len(self.cache) >= self.buffer_size:
            self._evict_page()
        self.cache[page_id] = page
        self.pin_count[page_id] = 1
        return page

    def _evict_page(self) -> None:
        """
        淘汰最久未使用且未被钉住的页（LRU）。
        """
        for evict_id, page in self.cache.items():
            if self.pin_count.get(evict_id, 0) == 0:
                if page.is_dirty:
                    self.flush_page(evict_id)
                self.cache.pop(evict_id, None)
                self.pin_count.pop(evict_id, None)
                self.dirty_pages.discard(evict_id)
                return
        raise RuntimeError('No unpinned page to evict!')

    def flush_all(self) -> None:
        """
        将所有脏页写回磁盘。
        """
        for page_id in list(self.dirty_pages):
            self.flush_page(page_id)

    def free_page(self, page_id: int) -> None:
        """
        只在缓存中删除页，不在物理层删除。
        """
        if page_id in self.cache:
            self.flush_page(page_id)
            del self.cache[page_id]
            self.pin_count.pop(page_id, None)
            self.dirty_pages.discard(page_id)

    def delete_table_pages(self, root_page_id: int) -> None:
        """
        从根节点开始，递归删除所有通过next_page_id链接的节点，并回收所有相关页。
        适用于B+树叶子链表或单链表结构。
        """
        visited = set()
        current_id = root_page_id
        while current_id is not None and current_id not in visited:
            visited.add(current_id)
            try:
                page = self.get_page(current_id)
                next_id = getattr(page, 'next_page_id', None)
                self.unpin_page(current_id, is_dirty=False)
                self.free_page(current_id)
                current_id = next_id if next_id != 0 else None
            except Exception as e:
                break
