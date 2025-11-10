"""
表空间/页文件管理模块。

职责：
- 管理物理页文件，提供分配、读取、写入、回收与删除能力
- 采用文件头维护空闲页链表（free list）

不改变原有实现，仅补充文档与类型注解。
"""

import os
import struct

class TablespaceManager:
    """
    物理页管理器，负责磁盘页的分配、读写和回收。
    文件头结构：
    - 4字节: 空闲页链表头（free_list_head，页号，0xFFFFFFFF表示无空闲页）
    其余为数据页，每页大小固定。
    每个空闲页的前4字节存下一个空闲页号。
    """
    FILE_HEADER_STRUCT = struct.Struct('I')  # 4字节，空闲页链表头
    FILE_HEADER_SIZE = FILE_HEADER_STRUCT.size
    PAGE_HEADER_STRUCT = struct.Struct('I')  # 4字节，空闲页的下一个空闲页号
    NO_FREE_PAGE = 0xFFFFFFFF

    def __init__(self, file_path: str, page_size: int = 4096):
        self.file_path = file_path
        self.page_size = page_size
        if not os.path.exists(file_path):
            with open(file_path, 'wb') as f:
                f.write(self.FILE_HEADER_STRUCT.pack(self.NO_FREE_PAGE))  # 初始化空闲链表头为NO_FREE_PAGE
        self._file = open(file_path, 'r+b')

    def _get_file_size(self) -> int:
        self._file.seek(0, os.SEEK_END)
        return self._file.tell()

    def _get_free_list_head(self) -> int:
        self._file.seek(0)
        return self.FILE_HEADER_STRUCT.unpack(self._file.read(self.FILE_HEADER_SIZE))[0]

    def _set_free_list_head(self, page_id: int) -> None:
        self._file.seek(0)
        self._file.write(self.FILE_HEADER_STRUCT.pack(page_id))
        self._file.flush()

    def read_page(self, page_id: int) -> bytes:
        offset = self.FILE_HEADER_SIZE + (page_id - 1) * self.page_size
        self._file.seek(offset)
        return self._file.read(self.page_size)

    def write_page(self, page_id: int, data: bytes) -> None:
        if len(data) != self.page_size:
            raise ValueError('Data size must equal page size.')
        offset = self.FILE_HEADER_SIZE + (page_id - 1) * self.page_size
        self._file.seek(offset)
        self._file.write(data)
        self._file.flush()

    def allocate_page(self) -> int:
        free_head = self._get_free_list_head()
        if free_head != self.NO_FREE_PAGE:
            # 有空闲页，复用
            offset = self.FILE_HEADER_SIZE + (free_head - 1) * self.page_size
            self._file.seek(offset)
            next_free = self.PAGE_HEADER_STRUCT.unpack(self._file.read(4))[0]
            self._set_free_list_head(next_free)
            return free_head
        else:
            # 没有空闲页，扩展文件
            file_size = self._get_file_size()
            page_id = ((file_size - self.FILE_HEADER_SIZE) // self.page_size) + 1
            self._file.seek(self.FILE_HEADER_SIZE + (page_id - 1) * self.page_size)
            self._file.write(b'\x00' * self.page_size)
            self._file.flush()
            return page_id

    def free_page(self, page_id: int) -> None:
        # 将该页的前4字节写为当前空闲链表头，然后更新链表头
        free_head = self._get_free_list_head()
        offset = self.FILE_HEADER_SIZE + (page_id - 1) * self.page_size
        self._file.seek(offset)
        self._file.write(self.PAGE_HEADER_STRUCT.pack(free_head))
        self._set_free_list_head(page_id)
        self._file.flush()

    def delete_file(self) -> None:
        """关闭并删除物理文件。"""
        self.close()
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def close(self) -> None:
        self._file.close()
