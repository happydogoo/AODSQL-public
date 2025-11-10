import os
import struct
from typing import Optional

class TablespaceManager:
    """
    管理物理存储文件（表空间），处理页的分配、释放、读取和写入。
    使用一个基于文件的链表来管理空闲页。
    """
    # 文件头只存储空闲页链表的头指针 (一个4字节的无符号整数)
    FILE_HEADER_STRUCT = struct.Struct('I')
    FILE_HEADER_SIZE = FILE_HEADER_STRUCT.size
    NULL_PAGE_ID = 0  # 特殊值，代表空闲链表的末尾

    def __init__(self, path: str, page_size: int = 4096):
        self.path = path
        self.page_size = page_size
        self._file = None
        
        is_new_file = not os.path.exists(path) or os.path.getsize(path) == 0

        # 以 'r+b' 模式打开文件，如果不存在会报错。所以我们先确保文件存在。
        if is_new_file:
            # 如果是新文件，先用 'w+b' 创建它
            self._file = open(self.path, 'w+b')
            self._set_free_list_head(self.NULL_PAGE_ID) # 写入初始文件头
            self.total_pages = 0
            self.next_page_id = 1
        else:
            # 如果文件已存在，用 'r+b' 打开
            self._file = open(self.path, 'r+b')
            file_size = os.path.getsize(self.path)
            self.total_pages = (file_size - self.FILE_HEADER_SIZE) // self.page_size
            self.next_page_id = self.total_pages + 1
    
    def _get_page_offset(self, page_id: int) -> int:
        """根据页ID计算文件内的偏移量 (页ID从1开始)"""
        if page_id < 1:
            raise ValueError("Page ID must be positive.")
        return self.FILE_HEADER_SIZE + (page_id - 1) * self.page_size

    def _get_free_list_head(self) -> int:
        self._file.seek(0)
        header_bytes = self._file.read(self.FILE_HEADER_SIZE)
        # 健壮性检查，防止文件损坏或过小
        if len(header_bytes) < self.FILE_HEADER_SIZE:
            raise IOError("Tablespace file header is corrupted or missing.")
        return self.FILE_HEADER_STRUCT.unpack(header_bytes)[0]

    def _set_free_list_head(self, page_id: int) -> None:
        self._file.seek(0)
        self._file.write(self.FILE_HEADER_STRUCT.pack(page_id))
        self._file.flush()

    def allocate_page(self) -> int:
        free_head = self._get_free_list_head()
        if free_head != self.NULL_PAGE_ID:
            # 如果空闲链表中有页面，则复用它
            page_offset = self._get_page_offset(free_head)
            self._file.seek(page_offset)
            # 在空闲页的开头，存储着下一个空闲页的ID
            next_free_bytes = self._file.read(self.FILE_HEADER_SIZE)
            next_free_head = self.FILE_HEADER_STRUCT.unpack(next_free_bytes)[0]
            self._set_free_list_head(next_free_head)
            return free_head
        else:
            # 如果没有空闲页，则在文件末尾分配一个新页
            page_id = self.next_page_id
            self.total_pages += 1
            self.next_page_id += 1
            # 通过写入一个空页来扩展文件
            empty_page_data = b'\x00' * self.page_size
            self.write_page(page_id, empty_page_data)
            return page_id

    def free_page(self, page_id: int) -> None:
        """将一个页面添加回空闲链表的头部"""
        current_head = self._get_free_list_head()
        page_offset = self._get_page_offset(page_id)
        self._file.seek(page_offset)
        # 将旧的链表头ID写入到这个被释放的页面中
        self._file.write(self.FILE_HEADER_STRUCT.pack(current_head))
        # 更新文件头，使其指向这个新释放的页面
        self._set_free_list_head(page_id)

    def read_page(self, page_id: int) -> bytes:
        offset = self._get_page_offset(page_id)
        self._file.seek(offset)
        return self._file.read(self.page_size)

    def write_page(self, page_id: int, data: bytes) -> None:
        if len(data) != self.page_size:
            raise ValueError(f"Data size {len(data)} does not match page size {self.page_size}")
        offset = self._get_page_offset(page_id)
        self._file.seek(offset)
        self._file.write(data)
        self._file.flush()

    def close(self) -> None:
        if self._file and not self._file.closed:
            self._file.close()
            self._file = None

    def delete_file(self) -> None:
        self.close() # 删除前确保文件已关闭
        if os.path.exists(self.path):
            os.remove(self.path)

    def __del__(self):
        # 确保对象被垃圾回收时，文件句柄也能被关闭
        self.close()