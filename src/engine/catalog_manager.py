"""
目录管理器（CatalogManager）

职责：
- 管理表、索引、视图、触发器等元数据的内存结构与持久化
- 提供增删改查接口，以及基础统计信息维护

说明：
- 本编辑仅补充文档与类型注解，不改变任何业务逻辑
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
# from loguru import logger
import json
import os
from .transaction.transaction import Transaction  # 新增导入

# CATALOG_FILE = 'catalog.json'

@dataclass
class ColumnInfo:
    column_name: str
    data_type: str
    not_null: bool = False
    default: Optional[Any] = None
    check: Optional[str] = None  # 存储表达式字符串
    is_primary_key: bool = False  # 新增主键标志
    def to_dict(self):
        return {
            'column_name': self.column_name,
            'data_type': self.data_type,
            'not_null': self.not_null,
            'default': self.default,
            'check': self.check,
            'is_primary_key': self.is_primary_key,  # 新增
        }

    @staticmethod
    def from_dict(d):
        return ColumnInfo(
            d['column_name'],
            d['data_type'],
            d.get('not_null', False),
            d.get('default', None),
            d.get('check', None),
            d.get('is_primary_key', False)  # 新增
        )

@dataclass
class IndexInfo:
    index_name: str
    file_name: str
    root_page_id: Optional[int] = None
    column_names: List[str] = field(default_factory=list)
    key_col_types: List[int] = field(default_factory=list)
    index_type: str = 'BTREE'
    is_unique: bool = False
    def to_dict(self):
        return {
            'index_name': self.index_name,
            'file_name': self.file_name,
            'root_page_id': self.root_page_id,
            'column_names': self.column_names,
            'key_col_types': self.key_col_types,
            'index_type': self.index_type,
            'is_unique': self.is_unique,
        }
    @staticmethod
    def from_dict(d):
        return IndexInfo(
            index_name=d['index_name'],
            file_name=d['file_name'],
            root_page_id=d.get('root_page_id'),
            column_names=d.get('column_names', []),
            key_col_types=d.get('key_col_types', []),
            index_type=d.get('index_type', 'BTREE'),
            is_unique=d.get('is_unique', False),
        )

@dataclass
class ViewInfo:
    view_name: str
    schema_name: str = 'public'  # 默认模式
    creator: str = 'system'  # 创建者
    definition: str = ''  # 视图的完整SELECT语句
    created_at: str = ''  # 创建时间
    is_updatable: bool = False  # 是否可更新
    
    def to_dict(self):
        return {
            'view_name': self.view_name,
            'schema_name': self.schema_name,
            'creator': self.creator,
            'definition': self.definition,
            'created_at': self.created_at,
            'is_updatable': self.is_updatable,
        }
    
    @staticmethod
    def from_dict(d):
        return ViewInfo(
            view_name=d['view_name'],
            schema_name=d.get('schema_name', 'public'),
            creator=d.get('creator', 'system'),
            definition=d['definition'],
            created_at=d.get('created_at', ''),
            is_updatable=d.get('is_updatable', False),
        )

@dataclass
class TriggerInfo:
    trigger_name: str
    table_name: str
    timing: str  # BEFORE, AFTER, INSTEAD OF
    events: List[str]  # INSERT, UPDATE, DELETE
    is_row_level: bool  # FOR EACH ROW
    when_condition: Optional[str] = None  # WHEN 条件
    trigger_body: List[str] = field(default_factory=list)  # BEGIN...END 块中的语句
    
    def to_dict(self):
        return {
            'trigger_name': self.trigger_name,
            'table_name': self.table_name,
            'timing': self.timing,
            'events': self.events,
            'is_row_level': self.is_row_level,
            'when_condition': self.when_condition,
            'trigger_body': self.trigger_body,
        }

    @staticmethod
    def from_dict(d):
        return TriggerInfo(
            d['trigger_name'],
            d['table_name'],
            d['timing'],
            d['events'],
            d['is_row_level'],
            d.get('when_condition', None),
            d.get('trigger_body', [])
        )

@dataclass
class TableInfo:
    table_name: str
    columns: List[ColumnInfo] = field(default_factory=list)
    file_name: str = None  # 新增字段，记录表文件名
    root_page_id: Optional[int] = None # 对于B+树索引很重要
    last_page_id: Optional[int] = None # 对于堆表快速插入很重要
    indexes: Dict[str, IndexInfo] = field(default_factory=dict) # index_name -> IndexInfo
    # 简化统计
    row_count: int = 0
    page_count: int = 0
    # 列级统计: column_name -> {"distinct": int, "nulls": int, "min": any, "max": any,
    #                          "histogram": list[(bucket_high, freq)], "mcv": list[(value, freq)]}
    column_stats: Dict[str, Dict] = field(default_factory=dict)

    def to_dict(self):
        return {
            'table_name': self.table_name,
            'columns': [col.to_dict() for col in self.columns],
            'file_name': self.file_name,
            'root_page_id': self.root_page_id,
            'last_page_id': self.last_page_id,
            'indexes': {k: v.to_dict() for k, v in self.indexes.items()},
            'row_count': self.row_count,
            'page_count': self.page_count,
            'column_stats': self.column_stats,
        }

    @staticmethod
    def from_dict(d):
        return TableInfo(
            table_name=d['table_name'],
            columns=[ColumnInfo.from_dict(col) for col in d['columns']],
            file_name=d.get('file_name', f"{d['table_name']}.db"),
            root_page_id=d.get('root_page_id'),
            last_page_id=d.get('last_page_id'),
            indexes={k: IndexInfo.from_dict(v) for k, v in d.get('indexes', {}).items()},
            row_count=d.get('row_count', 0),
            page_count=d.get('page_count', 0),
            column_stats=d.get('column_stats', {}),
        )

class CatalogManager:
    def __init__(self,catalog_path: str = 'catalog.json') -> None:
        self.catalog_path = catalog_path
        self.tables: Dict[str, TableInfo] = {}
        self.views: Dict[str, ViewInfo] = {}  # 视图元数据存储
        self.triggers: Dict[str, TriggerInfo] = {}  # 触发器元数据存储
        self._load_catalog()

    def _load_catalog(self) -> None:
        """从JSON文件加载目录到内存缓存。如果文件不存在则创建一个空的。"""
        if os.path.exists(self.catalog_path):
            try:
                if os.path.getsize(self.catalog_path) == 0:
                    # 空文件，初始化为空目录
                    self.tables = {}
                    self.views = {}
                    self._save_catalog()
                    return
                with open(self.catalog_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not isinstance(data, dict):
                        data = {}
                    
                    # 加载表信息
                    self.tables = {name: TableInfo.from_dict(tinfo) for name, tinfo in data.get('tables', {}).items()}
                    
                    # 加载视图信息
                    self.views = {name: ViewInfo.from_dict(vinfo) for name, vinfo in data.get('views', {}).items()}
                    
                    # 加载触发器信息
                    self.triggers = {name: TriggerInfo.from_dict(tinfo) for name, tinfo in data.get('triggers', {}).items()}
            except Exception:
                # 文件损坏或内容非法，回退为空目录
                self.tables = {}
                self.views = {}
                self._save_catalog()
        else:
            self.tables = {}
            self.views = {}
            self._save_catalog()

    def _save_catalog(self) -> None:
        """将内存中的目录缓存持久化到JSON文件。"""
        data = {
            'tables': {name: tinfo.to_dict() for name, tinfo in self.tables.items()},
            'views': {name: vinfo.to_dict() for name, vinfo in self.views.items()},
            'triggers': {name: tinfo.to_dict() for name, tinfo in self.triggers.items()}
        }
        with open(self.catalog_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def create_table(self, transaction: 'Transaction', table_name: str, columns: list[tuple[str, str]], file_name: str = None) -> None:
        """创建新的表。"""
        if table_name in self.tables:
            raise Exception(f"Table {table_name} already exists")
        column_infos = [ColumnInfo(c[0], c[1]) for c in columns]
        if file_name is None:
            file_name = f"{table_name}.db"
        self.tables[table_name] = TableInfo(table_name, column_infos, file_name=file_name, root_page_id=None, last_page_id=None)
        # self._save_catalog()  # 移除自动保存

    # --- 索引相关 ---

    def create_index(self, transaction: 'Transaction', table_name: str, index_name: str, column_names: list[str], file_name: str, key_col_types: list[int], index_type: str = 'BTREE', is_unique: bool = False) -> tuple[bool, str]:
        """
        创建索引，返回创建状态
        :return: (success: bool, message: str)
        """

        if table_name not in self.tables:
            return False, f"表 {table_name} 不存在"
        
        t = self.tables[table_name]
        if index_name in t.indexes:
            return False, f"索引 {index_name} 在表 {table_name} 上已存在"
        
        idx_info = IndexInfo(index_name, file_name, None, column_names, key_col_types, index_type, is_unique)
        t.indexes[index_name] = idx_info
        self._save_catalog()  # 保存索引信息到catalog.json
        print(f"[CatalogManager]: 索引 '{index_name}' 已在表 '{table_name}' 的列 {column_names} 上注册，文件: {file_name}。")
        return True, f"索引 '{index_name}' 创建成功"

    def has_index_on(self, table_name: str, column_name: str) -> bool:
        if table_name not in self.tables:
            return False
        # 检查是否有索引包含该列
        for index_info in self.tables[table_name].indexes.values():
            if column_name in index_info.column_names:
                return True
        return False

    def get_index_by_column(self, table_name: str, column_name: str) -> Optional[str]:
        if table_name not in self.tables:
            return None
        for index_name, index_info in self.tables[table_name].indexes.items():
            if column_name in index_info.column_names:
                return index_name
        return None

    def get_table(self,table_name: str) -> TableInfo:
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        return self.tables[table_name]

    def update_table_last_page(self, transaction: 'Transaction', table_name: str, last_page_id: int) -> None:
        """
        更新一张表的物理元数据（例如，最后页ID）。
        这是一个给 StorageEngine 内部使用的辅助方法。
        """
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        self.tables[table_name].last_page_id = last_page_id
        # self._save_catalog()  # 移除自动保存

    def update_table_root_page(self, transaction: 'Transaction', table_name: str, root_page_id: int) -> None:
        """更新表的根页ID。"""
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        self.tables[table_name].root_page_id = root_page_id
        # self._save_catalog()  # 移除自动保存

    def delete_table(self, transaction: 'Transaction', table_name: str) -> None:
        """删除指定表的元数据。"""
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        del self.tables[table_name]
        # self._save_catalog()  # 移除自动保存

    def list_tables(self) -> List[str]:
        """列出所有表名。"""
        return list(self.tables.keys())

    # --- 统计信息相关 ---
    def set_column_stats(
        self,
        transaction: 'Transaction',
        table_name: str,
        column_name: str,
        *,
        distinct: Optional[int] = None,
        nulls: Optional[int] = None,
        minimum: Optional[object] = None,
        maximum: Optional[object] = None,
        histogram: Optional[List[tuple]] = None,
        mcv: Optional[List[tuple]] = None,
    ) -> None:
        """为指定表列设置统计信息。

        参数只要提供就会被写入；未提供的不覆盖原值。
        histogram 采用 [(bucket_high, freq), ...]；mcv 采用 [(value, freq), ...]。
        """
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        t = self.tables[table_name]
        if column_name not in [c.column_name for c in t.columns]:
            raise Exception(f"Column {column_name} not found in table {table_name}")
        stats = t.column_stats.get(column_name, {})
        if distinct is not None:
            stats["distinct"] = int(distinct)
        if nulls is not None:
            stats["nulls"] = int(nulls)
        if minimum is not None:
            stats["min"] = minimum
        if maximum is not None:
            stats["max"] = maximum
        if histogram is not None:
            # 规范化为列表[List]，元素为 [bucket_high, freq]
            stats["histogram"] = [(bh, int(freq)) for bh, freq in histogram]
        if mcv is not None:
            stats["mcv"] = [(val, int(freq)) for val, freq in mcv]
        t.column_stats[column_name] = stats
        # self._save_catalog()  # 移除自动保存

    def get_column_stats(self, table_name: str, column_name: str) -> Optional[Dict]:
        """获取指定表列的统计信息"""
        if table_name not in self.tables:
            return None
        table = self.tables[table_name]
        if column_name not in [c.column_name for c in table.columns]:
            return None
        return table.column_stats.get(column_name)

    def update_table_stats(self, transaction: 'Transaction', table_name: str, row_count: int, page_count: int) -> None:
        """更新表的统计信息"""
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        table = self.tables[table_name]
        table.row_count = row_count
        table.page_count = page_count
        # self._save_catalog()  # 移除自动保存

    def inc_row_count(self, transaction: 'Transaction', table_name: str, delta: int) -> None:
        """
        增加或减少表的行数统计，并持久化。
        """
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        t = self.tables[table_name]
        t.row_count = getattr(t, 'row_count', 0) + delta
        # self._save_catalog()  # 移除自动保存
    
    def get_table_info(self, table_name: str) -> TableInfo:
        """获取表的详细信息"""
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        return self.tables[table_name]
    
    def get_all_tables(self) -> List[TableInfo]:
        """获取所有表的信息"""
        return list(self.tables.values())
    
    def get_table_indexes(self, table_name: str) -> List[Tuple[str, str, bool]]:
        """获取表的索引信息"""
        if table_name not in self.tables:
            return []
        
        table_info = self.tables[table_name]
        indexes = []
        
        # 遍历表的索引字典
        for index_name, index_info in table_info.indexes.items():
            for column_name in index_info.column_names:
                indexes.append((index_name, column_name, index_info.is_unique))
        
        return indexes
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        return table_name in self.tables

    def update_index_root_page(self, transaction: 'Transaction', table_name: str, index_name: str, root_page_id: int) -> None:
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        t = self.tables[table_name]
        if index_name not in t.indexes:
            raise Exception(f"Index {index_name} not found on table {table_name}")
        t.indexes[index_name].root_page_id = root_page_id
        # self._save_catalog()  # 移除自动保存

    def get_index_info(self, table_name: str, index_name: str) -> IndexInfo:
        if table_name not in self.tables:
            raise Exception(f"Table {table_name} not found")
        t = self.tables[table_name]
        if index_name not in t.indexes:
            raise Exception(f"Index {index_name} not found on table {table_name}")
        return t.indexes[index_name]
    

    # --- 视图管理相关方法 ---
    def create_view(self, view_name: str, definition: str, schema_name: str = 'public', 
                   creator: str = 'system', is_updatable: bool = False, transaction: 'Transaction' = None) -> None:
        """创建视图"""
        if view_name in self.views: del self.views[view_name]
        if view_name in self.views:
            raise Exception(f"View {view_name} already exists")
        
        import datetime
        view_info = ViewInfo(
            view_name=view_name,
            schema_name=schema_name,
            creator=creator,
            definition=definition,
            created_at=datetime.datetime.now().isoformat(),
            is_updatable=is_updatable
        )
        self.views[view_name] = view_info
        self._save_catalog()
        print(f"[CatalogManager]: 视图 '{view_name}' 已创建，定义: {definition}")
    
    def get_view(self, view_name: str) -> ViewInfo:
        """获取视图信息"""
        if view_name not in self.views:
            raise Exception(f"View {view_name} not found")
        return self.views[view_name]
    
    def view_exists(self, view_name: str) -> bool:
        """检查视图是否存在"""
        return view_name in self.views
    
    def delete_view(self, view_name: str, transaction: 'Transaction' = None) -> None:
        """删除视图"""
        if view_name not in self.views:
            raise Exception(f"View {view_name} not found")
        del self.views[view_name]
        self._save_catalog()
        print(f"[CatalogManager]: 视图 '{view_name}' 已删除")
        # 日志化已移除，留在real_storage_engine.py
    
    def update_view(self, view_name: str, definition: str, is_updatable: bool = None, transaction: 'Transaction' = None) -> None:
        """更新视图定义"""
        if view_name not in self.views:
            raise Exception(f"View {view_name} not found")
        
        view_info = self.views[view_name]
        view_info.definition = definition
        if is_updatable is not None:
            view_info.is_updatable = is_updatable
        
        self._save_catalog()
        print(f"[CatalogManager]: 视图 '{view_name}' 已更新，新定义: {definition}")
    
    def list_views(self) -> List[str]:
        """列出所有视图名"""
        return list(self.views.keys())
    
    def is_view_updatable(self, view_name: str) -> bool:
        """检查视图是否可更新"""
        if view_name not in self.views:
            return False
        return self.views[view_name].is_updatable
    
    # --- 触发器相关 ---
    def create_trigger(self, trigger_name: str, table_name: str, timing: str, events: list, 
                      is_row_level: bool, when_condition: Any, trigger_body: list, transaction: 'Transaction' = None) -> tuple[bool, str]:
        """创建触发器"""
        try:
            if trigger_name in self.triggers:
                return False, f"触发器 '{trigger_name}' 已存在"
            # 检查表是否存在
            if table_name not in self.tables:
                return False, f"表 '{table_name}' 不存在"
            # 转换触发器体为字符串列表
            trigger_body_str = []
            for stmt in trigger_body:
                trigger_body_str.append(str(stmt))
            # 创建触发器信息
            trigger_info = TriggerInfo(
                trigger_name=trigger_name,
                table_name=table_name,
                timing=timing,
                events=events,
                is_row_level=is_row_level,
                when_condition=str(when_condition) if when_condition else None,
                trigger_body=trigger_body_str
            )
            self.triggers[trigger_name] = trigger_info
            self._save_catalog()
            return True, f"触发器 '{trigger_name}' 创建成功"
        except Exception as e:
            return False, f"创建触发器失败: {str(e)}"

    def delete_trigger(self, trigger_name: str, transaction: 'Transaction' = None) -> tuple[bool, str]:
        """删除触发器"""
        try:
            if trigger_name not in self.triggers:
                return False, f"触发器 '{trigger_name}' 不存在"
            del self.triggers[trigger_name]
            self._save_catalog()
            return True, f"触发器 '{trigger_name}' 删除成功"
        except Exception as e:
            return False, f"删除触发器失败: {str(e)}"

    def update_trigger(self, trigger_name: str, table_name: str, timing: str, events: list, is_row_level: bool, when_condition: Any, trigger_body: list, transaction: 'Transaction' = None) -> tuple[bool, str]:
        """修改触发器（强制覆盖）"""
        # 如果已存在，先删除再创建
        if trigger_name in self.triggers:
            del self.triggers[trigger_name]
        return self.create_trigger(trigger_name, table_name, timing, events, is_row_level, when_condition, trigger_body, transaction=transaction)
    
    def list_triggers(self) -> List[TriggerInfo]:
        """列出所有触发器"""
        return list(self.triggers.values())
    
    def get_trigger(self, trigger_name: str) -> Optional[TriggerInfo]:
        """获取触发器信息"""
        return self.triggers.get(trigger_name)
    
    def trigger_exists(self, trigger_name: str) -> bool:
        """检查触发器是否存在"""
        return trigger_name in self.triggers

