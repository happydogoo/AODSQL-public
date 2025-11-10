# -*- coding: utf-8 -*-
"""
Operator definitions for the volcano/iterator model in the database engine.
向量化（批处理）和 schema 管理优化版。

本模块定义了数据库执行引擎的所有核心算子（Operator），包括：
- SeqScan: 顺序扫描
- Filter: 过滤
- Project: 投影
- Insert: 插入
- CreateTable: 创建表
- Update: 更新
- Delete: 删除

所有查询型算子采用向量化（批处理）执行，每次返回一个批次（batch）数据。
所有算子均支持 schema（输出模式）管理。
"""

from typing import List, Tuple, Dict, Iterator, Callable, Optional, Any
from src.engine.catalog_manager import CatalogManager

import time

from src.engine.transaction.transaction import Transaction
from loguru import logger


BATCH_SIZE = 1024

def profile_execution(func):
    """一个装饰器，用于分析算子next方法的执行性能"""
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, '_profile_data'):
            self._profile_data = {'time_ms': 0, 'rows': 0, 'calls': 0}
        
        start_time = time.perf_counter()
        result = func(self, *args, **kwargs)
        end_time = time.perf_counter()
        
        self._profile_data['time_ms'] += (end_time - start_time) * 1000
        self._profile_data['calls'] += 1
        if result:
            self._profile_data['rows'] += len(result)
        
        return result
    return wrapper

class Schema:
    """
    兼顾顺序和按名查找的表结构描述。
    支持清晰的字符串表示、迭代、数据验证。
    """
    columns: List[Tuple[str, str]]
    name_to_index: Dict[str, int]

    def __init__(self, columns: List[Tuple[str, str]]):
        """
        :param columns: list of (name, type)
        :raises: ValueError if columns 格式不合法
        """
        # 数据验证
        if not isinstance(columns, list):
            raise ValueError("Schema columns must be a list of (name, type)")
        for col in columns:
            if not (isinstance(col, tuple) and len(col) == 2 and isinstance(col[0], str) and isinstance(col[1], str)):
                raise ValueError(f"Invalid column definition: {col}. Each column must be (str, str)")
        self.columns = list(columns)
        self.name_to_index = {name: idx for idx, (name, _) in enumerate(self.columns)}

    def get_index(self, name: str) -> int:
        """按列名查找索引"""
        if name not in self.name_to_index:
            raise KeyError(f"Column name '{name}' not found in schema.")
        return self.name_to_index[name]

    def get_names(self) -> List[str]:
        """返回所有列名（有序）"""
        return [name for name, _ in self.columns]

    def get_types(self) -> List[str]:
        """返回所有类型（有序）"""
        return [typ for _, typ in self.columns]

    def __getitem__(self, idx: int) -> Tuple[str, str]:
        return self.columns[idx]

    def __len__(self) -> int:
        return len(self.columns)

    def __repr__(self) -> str:
        return f"Schema({self.columns})"

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        """允许直接迭代 Schema 对象以获取所有 Column"""
        return iter(self.columns)

class Operator:
    """
    所有算子的抽象基类。
    子类需实现 next() 或 execute() 方法。
    """
    schema: Optional[Schema]
    def __init__(self, metadata: Dict[str, Any] = None):
        self.schema: Optional[Schema] = None

        # 新增：用于存储可视化信息的元数据
        self.metadata: Dict[str, Any] = metadata or {}

        # 1. 【新增】为所有算子添加 transaction 属性
        self.transaction: Optional[Transaction] = None


    def next(self) -> Optional[List[Any]]:
        """
        查询型算子的批处理接口。
        :return: 一个批次的行（list of tuple），或 None（无更多数据）。
        """
        raise NotImplementedError

    def execute(self) -> Any:
        """
        终止型算子的执行接口。
        :return: 操作结果字符串。
        """
        raise NotImplementedError

class SeqScan(Operator):
    """
    顺序扫描算子。
    作为算子树的叶子节点，从存储引擎顺序读取表的全部数据。
    """
    table_name: str
    storage_engine: Any
    schema: Schema
    scanner: Optional[Iterator[Any]] # 初始化为 None

    def __init__(self, table_name: str, storage_engine: Any, schema: Schema, metadata: Dict[str, Any] = None):
        """
        :param table_name: 要扫描的表名。
        :param storage_engine: 存储引擎实例，需实现 scan(table_name)。
        :param schema: 表的模式（Schema 实例）。
        :param metadata: 元数据信息，包含成本、行数等估算信息。
        """
        super().__init__(metadata)
        self.table_name = table_name
        self.storage_engine = storage_engine
        self.schema = Schema(schema) if not isinstance(schema, Schema) else schema
        self.scanner: Optional[Iterator[Any]] = None  # 初始化为 None

    @profile_execution
    def next(self) -> Optional[List[Any]]:
        # 3. 【修改】在首次调用 next 时，使用事务对象初始化 scanner
        if self.scanner is None:
            if not self.transaction:
                raise Exception("SeqScan requires a valid transaction to execute.")
            self.scanner = iter(self.storage_engine.scan(self.transaction, self.table_name))
        batch: List[Any] = []
        try:
            for _ in range(BATCH_SIZE):
                batch.append(next(self.scanner))
        except StopIteration:
            pass
        return batch if batch else None

# In operator.py

class IndexScan(Operator):
    """
    【最终修复版】索引扫描算子。
    采用更简单直接的逻辑，确保数据能被正确获取和返回。
    """
    def __init__(self, table_name: str, storage_engine: Any, schema: Schema, 
                 index_name: str, predicate_key: tuple, metadata: Dict[str, Any] = None):
        super().__init__(metadata)
        self.table_name = table_name
        self.storage_engine = storage_engine
        self.schema = schema if isinstance(schema, Schema) else Schema(schema)
        self.index_name = index_name
        self.predicate_key = predicate_key
        
        # 使用一个简单的标志来判断是否已经处理完毕
        self._processed = False

    @profile_execution
    def next(self) -> Optional[List[Any]]:
        # 如果已经处理过，直接返回 None 表示结束
        if self._processed:
            return None

        # 首次调用时，执行所有逻辑
        # 1. 标记为已处理，这样下次调用就会直接结束
        self._processed = True

        # 2. 通过索引查找 row_id
        row_id = self.storage_engine.find_by_index(
            self.transaction, self.table_name, self.index_name, self.predicate_key
        )

        # 3. 如果没有找到 row_id，返回 None
        if row_id is None:
            return None
            
        # 4. 如果找到了 row_id，立即用它获取完整的行数据
        full_row = self.storage_engine.get_row(self.transaction, self.table_name, row_id)

        # 5. 如果成功获取到行数据，将其作为批次返回
        if full_row:
            # 将单个结果放入列表中，以符合批处理(batch)的格式
            return [full_row]
        else:
            # 如果 get_row 意外失败，也返回 None
            return None

class Filter(Operator):
    """
    过滤算子。
    对来自子节点的数据应用过滤条件，仅返回满足条件的行。
    """
    child: Operator
    condition: Callable[[Any], bool]
    schema: Schema
    _buffer: List[Any]

    def __init__(self, child: Operator, condition: Callable[[Any], bool], metadata: Dict[str, Any] = None):
        """
        :param child: 子算子，需实现 next()。
        :param condition: 过滤条件函数，接受一行数据，返回 bool。
        :param metadata: 元数据信息，包含成本、行数等估算信息。
        """
        super().__init__(metadata)
        self.child = child
        self.condition = condition
        self.schema = child.schema
        self._buffer: List[Any] = []

    @profile_execution
    def next(self) -> Optional[List[Any]]:
        while not self._buffer:
            child_batch = self.child.next()
            if child_batch is None:
                return None
            # child_batch: [(row_id, row_data), ...]
            # 条件函数需要处理 row_data 部分
            self._buffer = [row for row in child_batch if self.condition(row[1])]
        batch, self._buffer = self._buffer[:BATCH_SIZE], self._buffer[BATCH_SIZE:]
        return batch

class Project(Operator):
    """
    投影算子。
    对来自子节点的数据进行列选择或计算，仅返回指定的列。
    """
    child: Operator
    project_indices: List[int]
    schema: Schema

    def __init__(self, child: Operator, project_indices: List[int], metadata: Dict[str, Any] = None):
        super().__init__(metadata)
        self.child = child
        self.project_indices = project_indices
        
        # 清理列名，处理AST节点格式
        cleaned_columns = []
        for i in project_indices:
            col_name, col_type = self.child.schema[i]
            # 处理AST节点格式的列名
            if isinstance(col_name, str) and 'Identifier(' in col_name:
                import re
                match = re.search(r"value='([^']+)'", col_name)
                if match:
                    col_name = match.group(1)
                else:
                    col_name = col_name.split('.')[-1] if '.' in col_name else col_name
            elif isinstance(col_name, str) and '.' in col_name:
                col_name = col_name.split('.')[-1]
            cleaned_columns.append((col_name, col_type))
        
        self.schema = Schema(cleaned_columns)

    @profile_execution
    def next(self) -> Optional[List[Any]]:
        child_batch = self.child.next()
        if child_batch is None:
            return None
        # 保留row_id，投影row_data
        try:
            return [(row[0], tuple(row[1][i] for i in self.project_indices)) for row in child_batch]
        except IndexError as e:
            if child_batch:
                print(f"调试信息: project_indices={self.project_indices}")
                print(f"调试信息: 第一行数据={child_batch[0]}")
                print(f"调试信息: 第一行数据长度={len(child_batch[0][1]) if len(child_batch[0]) > 1 else 'N/A'}")
            raise e

class Insert(Operator):
    """
    插入算子。
    负责将一批数据插入到指定表。
    """
    table_name: str
    values: List[Tuple]
    storage_engine: Any
    schema: Optional[Schema]

    def __init__(self, table_name: str, values: List[Tuple], storage_engine: Any):
        """
        :param table_name: 目标表名。
        :param values: 待插入的多行数据（list of tuple）。
        :param storage_engine: 存储引擎实例，需实现 insert_row。
        """
        super().__init__()
        self.table_name = table_name
        self.values = values  # 可以是一行或多行
        self.storage_engine = storage_engine
        self.schema = None

    def execute(self) -> str:
        # 4. 【修改】在执行时，将 self.transaction 传递下去
        if not self.transaction:
            raise Exception("Insert requires a valid transaction to execute.")
        for row in self.values:
            self.storage_engine.insert_row(self.transaction, self.table_name, row)
        return f"{len(self.values)} rows inserted."

class CreateTable(Operator):
    """
    创建表算子。
    负责在系统目录和存储层创建新表。
    """
    def __init__(self, table_name: str, columns: List[Tuple[str, str]], storage_engine: Any):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.storage_engine = storage_engine
        self.schema = Schema(columns)

    def execute(self) -> str:
        if not self.transaction:
            raise Exception("CreateTable requires a valid transaction to execute.")
        self.storage_engine.create_table(self.transaction, self.table_name, self.columns)

        return f"Table '{self.table_name}' created."

class CreateIndex(Operator):
    """
    创建索引算子（执行层）。
    负责调用 CatalogManager 在指定表和列上注册索引。
    支持单列和多列索引。
    """
    def __init__(self, catalog_manager: CatalogManager, table_name: str, index_name: str, column_or_columns):
        super().__init__()
        self.catalog_manager = catalog_manager
        self.table_name = table_name
        self.index_name = index_name
        # 支持单列或多列
        if isinstance(column_or_columns, list):
            self.columns = column_or_columns
            self.column_name = column_or_columns[0]  # 保持向后兼容性
        else:
            self.column_name = column_or_columns
            self.columns = [column_or_columns]
        self.schema = None  # DDL 不产生上行数据

    def execute(self) -> str:
        if not self.transaction:
            raise Exception("CreateIndex requires a valid transaction to execute.")
        # 生成索引文件名
        file_name = f"{self.table_name}_{self.index_name}.idx"
        # 获取列的数据类型（这里简化处理，假设所有列都是INT类型）
        # 在实际实现中，应该从表信息中获取列的实际数据类型
        key_col_types = [1] * len(self.columns)  # 假设所有列都是INT类型
        
        # 调用catalog_manager创建索引，获取结果状态
        success, message = self.catalog_manager.create_index(
            self.transaction,  # <-- 新增
            self.table_name, 
            self.index_name, 
            self.columns,  # 使用列列表
            file_name, 
            key_col_types, 
            index_type='BTREE', 
            is_unique=False
        )
        
        # 根据结果生成返回消息
        if success:
            if len(self.columns) == 1:
                return f"✅ 索引 '{self.index_name}' 在 {self.table_name}({self.column_name}) 上创建成功"
            else:
                columns_str = ', '.join(self.columns)
                return f"✅ 索引 '{self.index_name}' 在 {self.table_name}({columns_str}) 上创建成功"
        else:
            # 友好的重复索引提示
            return f"ℹ️  {message}，无需重复创建"

class Update(Operator):
    """
    更新算子。
    负责执行 UPDATE 语句逻辑，对目标行应用更新规则并写回存储。
    """
    child: Operator
    table_name: str
    updates: Dict[int, Any]
    storage_engine: Any
    schema: Optional[Schema]

    def __init__(self, child: Operator, table_name: str, updates: Dict[int, Any], storage_engine: Any):
        """
        :param child: 提供需要被更新的行的子查询计划。
        :param table_name: 需要更新的表名。
        :param updates: dict，键为列索引，值为常量或函数。
        :param storage_engine: 存储引擎实例。
        """
        super().__init__()
        self.child = child
        self.table_name = table_name
        self.updates = updates
        self.storage_engine = storage_engine
        self.schema = None # Update算子不向上层产生数据

    def execute(self) -> str:
        if not self.transaction:
            raise Exception("Update requires a valid transaction to execute.")
        count = 0
        while True:
            batch = self.child.next()
            if batch is None:
                break
            for original_row in batch:
                row_id, row_data_tuple = original_row[0], original_row[1]
                new_row_list = list(row_data_tuple) # 转换为列表以便修改

                for col_index, value_or_func in self.updates.items():
                    # 检查 value_or_func 是不是一个可调用函数
                    if callable(value_or_func):
                        # 如果是函数，就用当前行的数据(row_data_tuple)作为输入来执行它
                        # 注意：求值函数需要的是未修改的原始行数据
                        updated_value = value_or_func(row_data_tuple)
                        new_row_list[col_index] = updated_value
                    else:
                        # 如果不是函数，就是普通字面量，直接赋值
                        new_row_list[col_index] = value_or_func
                
                new_row = tuple(new_row_list)
                self.storage_engine.update_row(self.transaction, self.table_name, row_id, new_row)
                count += 1
        return f"{count} rows updated."

class Delete(Operator):
    """
    删除算子。
    负责执行 DELETE 语句逻辑，删除目标行。
    """
    child: Operator
    table_name: str
    storage_engine: Any
    schema: Optional[Schema]

    def __init__(self, child: Operator, table_name: str, storage_engine: Any):
        """
        :param child: 提供需要被删除的行的子查询计划。
        :param table_name: 需要删除的表名。
        :param storage_engine: 存储引擎实例。
        """
        super().__init__()
        self.child = child
        self.table_name = table_name
        self.storage_engine = storage_engine
        self.schema = None # Delete算子不向上层产生数据

    def execute(self) -> str:
        if not self.transaction:
            raise Exception("Delete requires a valid transaction to execute.")
        # 先收集所有要删除的row_id
        to_delete = []
        while True:
            batch = self.child.next()
            if batch is None:
                break
            for row in batch:
                row_id = row[0]  # 假设第一个元素为 row_id
                to_delete.append(row_id)
        # 按(page_id, record_id)倒序排序
        to_delete.sort(key=lambda x: (x[0], -x[1]))
        count = 0
        for row_id in to_delete:
            self.storage_engine.delete_row(self.transaction, self.table_name, row_id)
            count += 1
        return f"{count} rows deleted."

class ShowTables(Operator):
    """
    查询型算子：返回所有表名。
    叶子节点，无子节点。
    """
    def __init__(self, catalog_manager):
        super().__init__()
        self.catalog_manager = catalog_manager
        self._fetched = False

    def next(self) -> Optional[List[Any]]:
        if self._fetched:
            return None
        table_names = self.catalog_manager.list_tables()
        self._fetched = True
        # 返回(row_id, (table_name,))，row_id用行号
        return [(idx, (name,)) for idx, name in enumerate(table_names)]
    
    def execute(self) -> str:
        """执行SHOW TABLES操作，返回表名列表"""
        table_names = self.catalog_manager.list_tables()
        if not table_names:
            return "No tables found."
        
        # 格式化输出
        result = []
        for i, table_name in enumerate(table_names, 1):
            result.append(f"{i}. {table_name}")
        
        return "\n".join(result)

class ShowViews(Operator):
    """
    查询型算子：返回所有视图名。
    叶子节点，无子节点。
    """
    def __init__(self, catalog_manager):
        super().__init__()
        self.catalog_manager = catalog_manager
        self._fetched = False

    def next(self) -> Optional[List[Any]]:
        if self._fetched:
            return None
        view_names = self.catalog_manager.list_views()
        self._fetched = True
        # 返回(row_id, (view_name,))，row_id用行号
        return [(idx, (name,)) for idx, name in enumerate(view_names)]



class ShowColumns(Operator):
    """
    查询型算子：返回指定表的列信息。
    叶子节点，无子节点。
    """
    def __init__(self, table_name: str, catalog_manager: CatalogManager):
        super().__init__()
        self.table_name = table_name
        self.catalog_manager = catalog_manager
        self._fetched = False

    def next(self) -> Optional[List[Any]]:
        if self._fetched:
            return None
        
        if not self.catalog_manager.table_exists(self.table_name):
            return None
            
        table_info = self.catalog_manager.get_table_info(self.table_name)
        columns = table_info.columns
        self._fetched = True
        
        # 返回(row_id, (column_name, data_type, nullable, default_value))
        return [(idx, (col.column_name, col.data_type, getattr(col, 'nullable', True), getattr(col, 'default_value', None))) 
                for idx, col in enumerate(columns)]

class ShowIndex(Operator):
    """
    查询型算子：返回指定表的索引信息。
    叶子节点，无子节点。
    """
    def __init__(self, table_name: str, catalog_manager: CatalogManager):
        super().__init__()
        self.table_name = table_name
        self.catalog_manager = catalog_manager
        self._fetched = False

    def next(self) -> Optional[List[Any]]:
        if self._fetched:
            return None
        
        if not self.catalog_manager.table_exists(self.table_name):
            return None
            
        # 获取表的索引信息
        indexes = self.catalog_manager.get_table_indexes(self.table_name)
        self._fetched = True
        
        # 返回(row_id, (index_name, column_name, unique))
        return [(idx, (idx_name, col_name, is_unique)) 
                for idx, (idx_name, col_name, is_unique) in enumerate(indexes)]

class DropTable(Operator):
    """
    终止型算子：删除指定表。
    叶子节点，无子节点。
    """
    def __init__(self, table_name: str, storage_engine: Any, if_exists: bool = False):
        super().__init__()
        self.table_name = table_name
        self.storage_engine = storage_engine
        self.if_exists = if_exists

    def execute(self) -> str:
        if not self.transaction:
            raise Exception("DropTable requires a valid transaction to execute.")
        return self.storage_engine.drop_table(self.transaction, self.table_name, self.if_exists)

class DropIndex(Operator):
    """
    终止型算子：删除指定索引。
    叶子节点，无子节点。
    """
    def __init__(self, index_name: str, storage_engine: Any, catalog_manager: Any):
        super().__init__()
        self.index_name = index_name
        self.storage_engine = storage_engine
        self.catalog_manager = catalog_manager

    def execute(self) -> str:
        if not self.transaction:
            raise Exception("DropIndex requires a valid transaction to execute.")
        # 通过遍历catalog找到索引所属的表
        table_name = None
        for table in self.catalog_manager.get_all_tables():
            if hasattr(table, 'indexes') and self.index_name in table.indexes:
                table_name = table.table_name
                break
        
        if table_name is None:
            return f"⚠️ 索引 '{self.index_name}' 不存在，无法删除"
        
        result = self.storage_engine.drop_index(self.transaction, table_name, self.index_name)
        return f"✅ 索引 '{self.index_name}' 已成功删除"

class Limit(Operator):
    """
    限制输出行数的算子，支持offset。
    """
    def __init__(self, child: Operator, limit: int, offset: int = 0):
        super().__init__()
        self.child = child
        self.limit = limit
        self.offset = offset
        self._rows_seen = 0
        self._rows_returned = 0
        self._exhausted = False
        self.schema = child.schema

    def next(self) -> Optional[List[Any]]:
        if self._exhausted or self._rows_returned >= self.limit:
            return None
        output_batch = []
        while len(output_batch) < BATCH_SIZE and self._rows_returned < self.limit:
            child_batch = self.child.next()
            if child_batch is None:
                break
            for row in child_batch:
                if self._rows_seen < self.offset:
                    self._rows_seen += 1
                    continue
                output_batch.append(row)
                self._rows_returned += 1
                self._rows_seen += 1
                if self._rows_returned >= self.limit:
                    break
        if not output_batch:
            self._exhausted = True
            return None
        return output_batch

class Sort(Operator):
    """
    阻塞型排序算子，支持多列多方向排序。
    """
    def __init__(self, child: Operator, sort_key_info: list):
        super().__init__()
        self.child = child
        self.sort_key_info = sort_key_info  # [(col_idx, 'ASC'/'DESC'), ...]
        self._sorted_data = None
        self._output_iter = None
        self.schema = child.schema

    def next(self) -> Optional[List[Any]]:
        if self._sorted_data is None:
            # 首次调用，拉取所有数据
            all_rows = []

            while True:
                batch = self.child.next()
                if batch is None:
                    break

                all_rows.extend(batch)
            
            # 排序
            def sort_key(row):
                # 强制(row_id, row_data)格式
                return tuple(
                    row[1][idx] if direction.upper() == 'ASC' else _SortDesc(row[1][idx])
                    for idx, direction in self.sort_key_info
                )
            
            self._sorted_data = sorted(all_rows, key=sort_key)
            self._output_iter = iter(self._sorted_data)
        # 分批输出
        batch = []
        try:
            for _ in range(BATCH_SIZE):
                batch.append(next(self._output_iter))
        except StopIteration:
            pass
        if not batch:
            return None
        return batch

class _SortDesc:
    def __init__(self, val):
        self.val = val
    def __lt__(self, other):
        return self.val > other.val
    def __eq__(self, other):
        return self.val == other.val

class HashAggregate(Operator):
    """
    阻塞型哈希分组聚合算子。
    由 PlanConverter 精确配置，只负责执行。
    """
    def __init__(self, child: Operator, group_by_indices: list, agg_expressions: list, output_schema: Schema):
        super().__init__()
        self.child = child
        self.group_by_indices = group_by_indices
        self.agg_expressions = agg_expressions
        self.schema = output_schema  # <-- 关键：直接接收并持有正确的输出 Schema
        self._results = []
        self._processed = False
        self._results_iterator = None

    def _build_hashtable(self):
        hashtable = {} 
        while True:
            batch = self.child.next()
            if batch is None:
                break
            for row_tuple in batch:
                row_data = row_tuple[1]
                group_key = tuple(row_data[i] for i in self.group_by_indices)
                if group_key not in hashtable:
                    hashtable[group_key] = [0] * len(self.agg_expressions)
                for i, (func, col_idx) in enumerate(self.agg_expressions):
                    if func.upper() == 'COUNT':
                        # COUNT(*) 或 COUNT(column) 都增加计数
                        hashtable[group_key][i] += 1
                    elif func.upper() == 'SUM':
                        try:
                            value = float(row_data[col_idx])
                            hashtable[group_key][i] += value
                        except (ValueError, TypeError):
                            pass  # 忽略无效值
                    elif func.upper() == 'AVG':
                        try:
                            value = float(row_data[col_idx])
                            # 存储 (sum, count) 用于计算平均值
                            if hashtable[group_key][i] == 0:
                                hashtable[group_key][i] = (value, 1)
                            else:
                                sum_val, count_val = hashtable[group_key][i]
                                hashtable[group_key][i] = (sum_val + value, count_val + 1)
                        except (ValueError, TypeError):
                            pass
                    elif func.upper() == 'MIN':
                        try:
                            value = float(row_data[col_idx])
                            if hashtable[group_key][i] == 0:
                                hashtable[group_key][i] = value
                            else:
                                hashtable[group_key][i] = min(hashtable[group_key][i], value)
                        except (ValueError, TypeError):
                            pass
                    elif func.upper() == 'MAX':
                        try:
                            value = float(row_data[col_idx])
                            if hashtable[group_key][i] == 0:
                                hashtable[group_key][i] = value
                            else:
                                hashtable[group_key][i] = max(hashtable[group_key][i], value)
                        except (ValueError, TypeError):
                            pass
        
        row_id_counter = 0
        for group_key, agg_values in hashtable.items():
            # 处理聚合结果，特别是AVG函数
            processed_values = []
            for i, value in enumerate(agg_values):
                if isinstance(value, tuple) and len(value) == 2:
                    # AVG函数的结果，计算平均值
                    sum_val, count_val = value
                    if count_val > 0:
                        processed_values.append(sum_val / count_val)
                    else:
                        processed_values.append(0.0)
                else:
                    processed_values.append(value)
            
            final_row_data = tuple(group_key) + tuple(processed_values)
            self._results.append((row_id_counter, final_row_data))
            row_id_counter += 1

    def next(self) -> Optional[List[Any]]:
        if not self._processed:
            self._build_hashtable()
            self._processed = True
            self._results_iterator = iter(self._results)        
        batch = []
        try:
            # BATCH_SIZE 是您在 operator.py 顶部定义的
            for _ in range(BATCH_SIZE):
                batch.append(next(self._results_iterator))
        except StopIteration:
            pass
        return batch if batch else None

class NestedLoopJoin(Operator):
    """
    嵌套循环连接算子，支持笛卡尔积和条件连接。
    """
    def __init__(self, left_child: Operator, right_child: Operator, condition=None):
        super().__init__()
        self.left_child = left_child
        self.right_child = right_child
        self.condition = condition
        self.schema = None
        self._left_data = []
        self._right_data = []
        self._processed = False
        self._current_left_index = 0
        self._current_right_index = 0
        self._output_buffer = []
    
    def next(self) -> Optional[List[Any]]:
        if not self._processed:
            self._build_data()
            self._processed = True
        
        if self._current_left_index >= len(self._left_data):
            return None
        
        # 获取当前左表行
        left_row = self._left_data[self._current_left_index]
        
        # 遍历右表行
        if self._current_right_index < len(self._right_data):
            right_row = self._right_data[self._current_right_index]
            
            # 检查连接条件
            if self.condition is None or self._evaluate_condition(left_row, right_row):
                # 合并左右表行
                merged_row = (left_row[0], left_row[1] + right_row[1])
                self._current_right_index += 1
                return [merged_row]
            else:
                # 条件不满足，移动到下一个右表行
                self._current_right_index += 1
                return self.next()
        else:
            # 当前左表行的所有右表行都已处理，移动到下一个左表行
            self._current_left_index += 1
            self._current_right_index = 0
            return self.next()
    
    def _build_data(self):
        """构建左右表数据"""
        # 扫描左表
        while True:
            left_batch = self.left_child.next()
            if left_batch is None:
                break
            for left_row in left_batch:
                self._left_data.append(left_row)
        
        # 扫描右表
        while True:
            right_batch = self.right_child.next()
            if right_batch is None:
                break
            for right_row in right_batch:
                self._right_data.append(right_row)
        
        # 设置输出schema
        if self._left_data and self._right_data:
            left_schema = self.left_child.schema
            right_schema = self.right_child.schema
            self.schema = Schema(left_schema.columns + right_schema.columns)
    
    def _evaluate_condition(self, left_row, right_row):
        """评估连接条件"""
        if self.condition is None:
            return True
        
        # 创建合并的行数据用于条件评估
        merged_data = left_row[1] + right_row[1]
        fake_row = (None, merged_data)
        
        try:
            return self.condition(fake_row)
        except:
            return False


class HashJoin(Operator):
    """
    阻塞型哈希连接算子，支持等值连接。
    """
    def __init__(self, left_child: Operator, right_child: Operator, left_key_indices: list, right_key_indices: list):
        super().__init__()
        self.left_child = left_child
        self.right_child = right_child
        self.left_key_indices = left_key_indices
        self.right_key_indices = right_key_indices
        self._hashtable = None
        self._probe_iter = None
        self._output_buffer = []
        self.schema = Schema(self.left_child.schema.columns + self.right_child.schema.columns)

    def next(self) -> Optional[List[Any]]:
        if self._hashtable is None:
            # 构建右表哈希表
            self._hashtable = {}
            while True:
                batch = self.right_child.next()
                if batch is None:
                    break
                for row in batch:
                    row_id, row_data = row  # 强制(row_id, row_data)格式
                    key = tuple(row_data[idx] for idx in self.right_key_indices)
                    self._hashtable.setdefault(key, []).append(row_data)
            self._probe_iter = self._probe_left()
        # 输出缓冲区
        batch = []
        try:
            for _ in range(BATCH_SIZE):
                batch.append(next(self._probe_iter))
        except StopIteration:
            pass
        if not batch:
            return None
        return batch

    def _probe_left(self):
        idx = 0
        while True:
            left_batch = self.left_child.next()
            if left_batch is None:
                break
            for left_row in left_batch:
                left_row_id, left_data = left_row  # 强制(row_id, row_data)格式
                key = tuple(left_data[idx] for idx in self.left_key_indices)
                matches = self._hashtable.get(key, [])
                for right_data in matches:
                    yield (idx, left_data + right_data)
                    idx += 1 

class Explain(Operator):
    """
    EXPLAIN 算子。
    一个终止型算子，负责将子计划格式化为人类可读的字符串。
    """
    def __init__(self, child: Operator):
        super().__init__()
        self.child = child
        self.schema = Schema([('Execution Plan', 'str')])  # 定义输出格式

    def _format_plan(self, node: Operator, indent: str = "") -> str:
        """
        递归格式化算子树。
        """
        plan_str = f"{indent}-> {type(node).__name__}\n"
        # 可根据需要添加更多细节，如条件、表名等
        # 递归处理子节点
        if hasattr(node, 'child') and getattr(node, 'child') is not None:
            plan_str += self._format_plan(node.child, indent + "   ")
        if hasattr(node, 'left_child') and getattr(node, 'left_child') is not None:
            plan_str += self._format_plan(node.left_child, indent + "   ")
        if hasattr(node, 'right_child') and getattr(node, 'right_child') is not None:
            plan_str += self._format_plan(node.right_child, indent + "   ")
        return plan_str

    def execute(self) -> str:
        """
        执行EXPLAIN操作，返回格式化后的计划字符串。
        """
        return self._format_plan(self.child)
# 视图
class CreateView(Operator):
    """
    物理算子：创建视图。
    这是一个终止型算子，不产生数据流。
    """
    def __init__(self, view_name: str, definition: str, schema_name: str = 'public', 
                 creator: str = 'system', is_updatable: bool = False, catalog_manager: Any = None):
        super().__init__()
        self.view_name = view_name
        self.definition = definition
        self.schema_name = schema_name
        self.creator = creator
        self.is_updatable = is_updatable
        self.catalog_manager = catalog_manager
        self.schema = Schema([])  # 终止型算子，无输出模式

    def execute(self):
        """
        执行创建视图的操作。
        """
        try:
            logger.debug(f"✅ 正在创建视图: {self.view_name}")
            logger.debug(f"   定义: {self.definition}")
            logger.debug(f"   模式: {self.schema_name}")
            logger.debug(f"   创建者: {self.creator}")
            logger.debug(f"   可更新: {self.is_updatable}")
            if self.catalog_manager:
                self.catalog_manager.create_view(
                    self.view_name,
                    self.definition,
                    self.schema_name,
                    self.creator,
                    self.is_updatable
                )
                return f"视图 '{self.view_name}' 创建成功。"
            else:
                return f"视图 '{self.view_name}' 创建失败：缺少存储引擎。"
        except Exception as e:
            return f"视图 '{self.view_name}' 创建失败：{str(e)}"
    
    def __repr__(self):
        return f"CreateView(view_name='{self.view_name}')"

class DropView(Operator):
    """
    物理算子：删除视图。
    这是一个终止型算子，不产生数据流。
    """
    def __init__(self, view_name: str, storage_engine: Any = None):
        super().__init__()
        self.view_name = view_name
        self.storage_engine = storage_engine
        self.schema = Schema([])  # 终止型算子，无输出模式

    def execute(self):
        """
        执行删除视图的操作。
        """
        try:
            logger.debug(f"✅ 正在删除视图: {self.view_name}")
            if self.storage_engine:
                self.storage_engine.delete_view(self.transaction, self.view_name)
                return f"视图 '{self.view_name}' 删除成功。"
            else:
                return f"视图 '{self.view_name}' 删除失败：缺少存储引擎。"
        except Exception as e:
            return f"视图 '{self.view_name}' 删除失败：{str(e)}"
    
    def __repr__(self):
        return f"DropView(view_name='{self.view_name}')"

class AlterView(Operator):
    """
    物理算子：修改视图。
    这是一个终止型算子，不产生数据流。
    """
    def __init__(self, view_name: str, definition: str, is_updatable: Optional[bool] = None, 
                 storage_engine: Any = None):
        super().__init__()
        self.view_name = view_name
        self.definition = definition
        self.is_updatable = is_updatable
        self.storage_engine = storage_engine
        self.schema = Schema([])  # 终止型算子，无输出模式

    def execute(self):
        """
        执行修改视图的操作。
        """
        try:
            logger.debug(f"✅ 正在修改视图: {self.view_name}")
            logger.debug(f"   新定义: {self.definition}")
            logger.debug(f"   可更新: {self.is_updatable}")
            if self.storage_engine:
                self.storage_engine.alter_view(
                    self.transaction,
                    self.view_name,
                    self.definition,
                    is_updatable=self.is_updatable
                )
                return f"视图 '{self.view_name}' 修改成功。"
            else:
                return f"视图 '{self.view_name}' 修改失败：缺少存储引擎。"
        except Exception as e:
            return f"视图 '{self.view_name}' 修改失败：{str(e)}"
    
    def __repr__(self):
        return f"AlterView(view_name='{self.view_name}')"

# 触发器
class CreateTrigger(Operator):
    """
    物理算子：创建触发器。
    这是一个终止型算子，不产生数据流。
    """
    def __init__(self, trigger_name: str, table_name: str, timing: str, events: list, 
                 is_row_level: bool, when_condition: any, trigger_body: list, storage_engine: any = None):
        super().__init__()
        self.trigger_name = trigger_name
        self.table_name = table_name
        self.timing = timing
        self.events = events
        self.is_row_level = is_row_level
        self.when_condition = when_condition
        self.trigger_body = trigger_body
        self.storage_engine = storage_engine
        self.schema = Schema([])  # 终止型算子，无输出模式

    def execute(self):
        """
        执行创建触发器的操作。
        """
        try:
            logger.debug(f"✅ 正在创建触发器: {self.trigger_name}")
            logger.debug(f"   表名: {self.table_name}")
            logger.debug(f"   时机: {self.timing}")
            logger.debug(f"   事件: {', '.join(self.events)}")
            logger.debug(f"   行级: {self.is_row_level}")
            if self.storage_engine:
                self.storage_engine.create_trigger(
                    self.transaction,
                    self.trigger_name,
                    self.table_name,
                    self.timing,
                    self.events,
                    self.is_row_level,
                    self.when_condition,
                    self.trigger_body
                )
                return f"触发器 '{self.trigger_name}' 创建成功。"
            else:
                return f"触发器 '{self.trigger_name}' 创建失败：缺少存储引擎。"
        except Exception as e:
            return f"❌ 创建触发器 '{self.trigger_name}' 失败: {str(e)}"
    
    def __repr__(self):
        return f"CreateTrigger(trigger_name='{self.trigger_name}', table_name='{self.table_name}')"

class AlterTrigger(Operator):
    """
    物理算子：修改触发器。
    这是一个终止型算子，不产生数据流。
    """
    def __init__(self, trigger_name: str, table_name: str, timing: str, events: list, 
                 is_row_level: bool, when_condition: any, trigger_body: list, storage_engine: any = None):
        super().__init__()
        self.trigger_name = trigger_name
        self.table_name = table_name
        self.timing = timing
        self.events = events
        self.is_row_level = is_row_level
        self.when_condition = when_condition
        self.trigger_body = trigger_body
        self.storage_engine = storage_engine
        self.schema = Schema([])  # 终止型算子，无输出模式

    def execute(self):
        """
        执行修改触发器的操作。
        """
        try:
            logger.debug(f"✅ 正在修改触发器: {self.trigger_name}")
            logger.debug(f"   表名: {self.table_name}")
            logger.debug(f"   时机: {self.timing}")
            logger.debug(f"   事件: {', '.join(self.events)}")
            logger.debug(f"   行级: {self.is_row_level}")
            if self.storage_engine:
                self.storage_engine.alter_trigger(
                    self.transaction,
                    self.trigger_name,
                    self.table_name,
                    self.timing,
                    self.events,
                    self.is_row_level,
                    self.when_condition,
                    self.trigger_body
                )
                return f"触发器 '{self.trigger_name}' 修改成功。"
            else:
                return f"触发器 '{self.trigger_name}' 修改失败：缺少存储引擎。"
        except Exception as e:
            return f"❌ 修改触发器 '{self.trigger_name}' 失败: {str(e)}"
    
    def __repr__(self):
        return f"AlterTrigger(trigger_name='{self.trigger_name}', table_name='{self.table_name}')"

class DropTrigger(Operator):
    """
    物理算子：删除触发器。
    这是一个终止型算子，不产生数据流。
    """
    def __init__(self, trigger_name: str, storage_engine: any = None):
        super().__init__()
        self.trigger_name = trigger_name
        self.storage_engine = storage_engine
        self.schema = Schema([])  # 终止型算子，无输出模式

    def execute(self):
        """
        执行删除触发器的操作。
        """
        try:
            logger.debug(f"✅ 正在删除触发器: {self.trigger_name}")
            if self.storage_engine:
                self.storage_engine.delete_trigger(self.transaction, self.trigger_name)
                return f"触发器 '{self.trigger_name}' 删除成功。"
            else:
                return f"触发器 '{self.trigger_name}' 删除失败：缺少存储引擎。"
        except Exception as e:
            return f"❌ 删除触发器 '{self.trigger_name}' 失败: {str(e)}"
    
    def __repr__(self):
        return f"DropTrigger(trigger_name='{self.trigger_name}')"

class ShowTriggers(Operator):
    """
    物理算子：显示触发器。
    这是一个查询型算子，通过 next() 接口返回数据。
    """
    def __init__(self, catalog_manager: CatalogManager = None):
        super().__init__()
        self.catalog_manager = catalog_manager
        # 定义输出的 Schema
        self.schema = Schema([
            ('trigger_name', 'VARCHAR'),
            ('table_name', 'VARCHAR'),
            ('timing', 'VARCHAR'),
            ('events', 'VARCHAR')
        ])
        self._fetched = False

    def next(self) -> Optional[List[Any]]:
        """
        返回所有触发器的信息。
        """
        if self._fetched or not self.catalog_manager:
            return None

        try:
            triggers = self.catalog_manager.list_triggers()  # 假设 catalog_manager 有此方法
            self._fetched = True
            
            # 将触发器信息格式化为 (row_id, (data_tuple))
            output_batch = []
            for i, trigger in enumerate(triggers):
                # 假设每个 trigger 对象有以下属性
                row_data = (
                    trigger.trigger_name,
                    trigger.table_name,
                    trigger.timing,
                    ', '.join(trigger.events)
                )
                output_batch.append((i, row_data))
            return output_batch
        except Exception as e:
            print(f"❌ 显示触发器列表失败: {str(e)}")
            self._fetched = True
            return None
    
    def __repr__(self):
        return "ShowTriggers()"

# --- 游标相关物理操作符 ---
class DeclareCursor(Operator):
    """声明游标物理操作符"""
    def __init__(self, cursor_name: str, query_plan: 'Operator', cli_interface=None):
        super().__init__()
        self.cursor_name = cursor_name
        self.query_plan = query_plan
        self.cli_interface = cli_interface
        self.schema = None  # DDL不向上层产生数据
    
    def execute(self) -> str:
        """执行声明游标操作"""
        try:
            if self.cli_interface:
                if self.cursor_name in self.cli_interface.cursors:
                    return f"❌ 错误: 游标 '{self.cursor_name}' 已存在。"
                
                # 创建游标信息并存储
                from cli.cli_interface import CursorInfo, CursorStatus
                cursor_info = CursorInfo(
                    name=self.cursor_name,
                    plan=self.query_plan,
                    status=CursorStatus.DECLARED
                )
                self.cli_interface.cursors[self.cursor_name] = cursor_info
                return f"✅ 游标 '{self.cursor_name}' 已声明。"
            else:
                return f"❌ 错误: CLI接口未初始化。"
        except Exception as e:
            return f"❌ 声明游标 '{self.cursor_name}' 失败: {str(e)}"
    
    def __repr__(self):
        return f"DeclareCursor({self.cursor_name})"

class OpenCursor(Operator):
    """打开游标物理操作符"""
    def __init__(self, cursor_name: str, cli_interface=None):
        super().__init__()
        self.cursor_name = cursor_name
        self.cli_interface = cli_interface
        self.schema = None  # DDL不向上层产生数据
    
    def execute(self) -> str:
        """执行打开游标操作"""
        try:
            if self.cli_interface:
                if self.cursor_name not in self.cli_interface.cursors:
                    return f"❌ 错误: 游标 '{self.cursor_name}' 不存在。"
                
                from cli.cli_interface import CursorStatus
                self.cli_interface.cursors[self.cursor_name].status = CursorStatus.OPEN
                return f"✅ 游标 '{self.cursor_name}' 已打开。"
            else:
                return f"❌ 错误: CLI接口未初始化。"
        except Exception as e:
            return f"❌ 打开游标 '{self.cursor_name}' 失败: {str(e)}"
    
    def __repr__(self):
        return f"OpenCursor({self.cursor_name})"


class FetchCursor(Operator):
    """获取游标物理操作符"""
    def __init__(self, cursor_name: str, cli_interface=None):
        super().__init__()
        self.cursor_name = cursor_name
        self.cli_interface = cli_interface
        self.schema = None  # 查询型操作符，schema由内部查询决定
    
    def execute(self) -> str:
        """执行获取游标操作"""
        try:
            if self.cli_interface:
                cursor_info = self.cli_interface.cursors.get(self.cursor_name)
                if not cursor_info or cursor_info.status != CursorStatus.OPEN:
                    return f"❌ 错误: 游标 '{self.cursor_name}' 未打开或不存在。"
                
                # 确保有事务上下文
                if not hasattr(self.cli_interface, 'system_manager') or not self.cli_interface.system_manager:
                    return f"❌ 错误: 缺少系统管理器，无法执行游标操作。"
                
                # 从游标的查询计划中获取下一批数据
                batch = cursor_info.plan.next()
                if batch:
                    # 显示结果
                    if hasattr(self.cli_interface, 'display') and self.cli_interface.display:
                        self.cli_interface.display.display_results(batch, cursor_info.plan.schema)
                    else:
                        # 简单的结果显示
                        for row_id, row_data in batch:
                            print(f"Row {row_id}: {row_data}")
                    return f"✅ 从游标 '{self.cursor_name}' 获取了 {len(batch)} 行数据。"
                else:
                    return "(无更多行)"
            else:
                return f"❌ 错误: CLI接口未初始化。"
        except Exception as e:
            return f"❌ 从游标 '{self.cursor_name}' 获取数据失败: {str(e)}"
    
    def __repr__(self):
        return f"FetchCursor({self.cursor_name})"

class CloseCursor(Operator):
    """关闭游标物理操作符"""
    def __init__(self, cursor_name: str, cli_interface=None):
        super().__init__()
        self.cursor_name = cursor_name
        self.cli_interface = cli_interface
        self.schema = None  # DDL不向上层产生数据
    
    def execute(self) -> str:
        """执行关闭游标操作"""
        try:
            if self.cli_interface:
                if self.cursor_name in self.cli_interface.cursors:
                    del self.cli_interface.cursors[self.cursor_name]
                    return f"✅ 游标 '{self.cursor_name}' 已关闭。"
                else:
                    return f"❌ 错误: 游标 '{self.cursor_name}' 不存在。"
            else:
                return f"❌ 错误: CLI接口未初始化。"
        except Exception as e:
            return f"❌ 关闭游标 '{self.cursor_name}' 失败: {str(e)}"
    
    def __repr__(self):
        return f"CloseCursor({self.cursor_name})"


class BeginTransaction(Operator):
    """开始事务操作符"""
    
    def __init__(self):
        super().__init__()
        self.type = "BEGIN_TRANSACTION"
    
    def execute(self):
        """执行开始事务操作"""
        return "Transaction started"
    
    def __repr__(self):
        return "BeginTransaction()"


class CommitTransaction(Operator):
    """提交事务操作符"""
    
    def __init__(self):
        super().__init__()
        self.type = "COMMIT_TRANSACTION"
    
    def execute(self):
        """执行提交事务操作"""
        return "Transaction committed"
    
    def __repr__(self):
        return "CommitTransaction()"


class RollbackTransaction(Operator):
    """回滚事务操作符"""
    
    def __init__(self):
        super().__init__()
        self.type = "ROLLBACK_TRANSACTION"
    
    def execute(self):
        """执行回滚事务操作"""
        return "Transaction rolled back"
    
    def __repr__(self):
        return "RollbackTransaction()"

