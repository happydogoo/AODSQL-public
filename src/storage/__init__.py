"""
Storage 子系统：页面模型、缓冲池与表空间管理。

模块清单：
- page: 页面抽象与堆页实现
- btreepage: B+树内部/叶子页
- buffer: 缓冲池（LRU、脏页、LSN 刷新）
- tablespace_manager: 物理页文件管理
"""

