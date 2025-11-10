from src.storage.page import HeapPage
from src.engine.transaction.transaction import Transaction

class HeapFileManager:
    def __init__(self, buffer_pool, table_info, catalog_manager):
        self.bp = buffer_pool
        self.table_info = table_info
        self.catalog_manager = catalog_manager
        self.page_cls = HeapPage

    def insert_record(self, transaction: Transaction, record_bytes: bytes, lsn: int):
        if self.table_info.last_page_id is None:
            page = self.bp.new_page(page_cls=self.page_cls)
            page_id = page.page_id
            self.table_info.root_page_id = page_id
            self.table_info.last_page_id = page_id
        else:
            page_id = self.table_info.last_page_id
            page = self.bp.get_page(page_id)
            if page.is_full(len(record_bytes)):
                self.bp.unpin_page(page_id, is_dirty=False)
                page = self.bp.new_page(page_cls=self.page_cls)
                page_id = page.page_id
                self.table_info.last_page_id = page_id
        try:
            record_id = page.insert_record(transaction, record_bytes, lsn)
            return (page_id, record_id)
        finally:
            self.bp.unpin_page(page_id, is_dirty=True)

    def update_record(self, transaction: Transaction, row_id: tuple, record_bytes: bytes, lsn: int):
        page_id, record_id = row_id
        page = self.bp.get_page(page_id)
        try:
            page.update_record(transaction, record_id, record_bytes, lsn)
        finally:
            self.bp.unpin_page(page_id, is_dirty=True)

    def delete_record(self, transaction: Transaction, row_id: tuple, record_size: int, lsn: int):
        page_id, record_id = row_id
        page = self.bp.get_page(page_id)
        try:
            page.mark_as_deleted(transaction, record_id, record_size, lsn)
        finally:
            self.bp.unpin_page(page_id, is_dirty=True)

    def get_record(self, transaction: Transaction, row_id: tuple, record_size: int) -> bytes:
        page_id, record_id = row_id
        page = self.bp.get_page(page_id)
        try:
            is_valid, row_bytes = page.get_record(record_id, record_size)
            if not is_valid:
                raise Exception(f"Record {row_id} not found or has been deleted.")
            return row_bytes
        finally:
            self.bp.unpin_page(page_id, is_dirty=False)

    def scan(self, transaction: Transaction, record_size: int):
        page_id = self.table_info.root_page_id
        if page_id is None:
            return
        current_page_id = page_id
        while current_page_id is not None and current_page_id != 0:
            page = self.bp.get_page(current_page_id)
            try:
                for rid in range(page.key_count):
                    is_valid, row_bytes = page.get_record(rid, record_size)
                    if not is_valid:
                        continue
                    yield (current_page_id, rid), row_bytes
                next_page_id = page.next_page_id
            finally:
                self.bp.unpin_page(current_page_id, is_dirty=False)
            current_page_id = next_page_id 

    def find_space_for_record(self, record_bytes: bytes):
        # 只定位插入位置，不做物理写入
        if self.table_info.last_page_id is None:
            page = self.bp.new_page(page_cls=self.page_cls)
            page_id = page.page_id
            self.table_info.root_page_id = page_id
            self.table_info.last_page_id = page_id
        else:
            page_id = self.table_info.last_page_id
            page = self.bp.get_page(page_id)
            if page.is_full(len(record_bytes)):
                self.bp.unpin_page(page_id, is_dirty=False)
                page = self.bp.new_page(page_cls=self.page_cls)
                page_id = page.page_id
                self.table_info.last_page_id = page_id
        # 只返回(page_id, 下一个可用slot)
        record_id = page.key_count
        self.bp.unpin_page(page_id, is_dirty=False)
        return (page_id, record_id)

    def insert_record_at(self, transaction: Transaction, row_id: tuple, record_bytes: bytes, lsn: int):
        page_id, record_id = row_id
        page = self.bp.get_page(page_id)
        try:
            # 直接在指定slot插入
            page.insert_record_at(transaction, record_id, record_bytes, lsn)
        finally:
            self.bp.unpin_page(page_id, is_dirty=True) 