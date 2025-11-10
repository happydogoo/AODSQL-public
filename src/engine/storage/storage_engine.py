# In engine/storage_engine.py
from typing import Iterator, Tuple, Dict, Any

class StorageEngine:
    """
    第一阶段存储引擎的完整接口定义。
    
    该接口提供了支持基础CRUD操作和表管理的全部方法，
    是完成第一阶段里程碑所必需的最小功能集。
    """

    # --- 表结构管理 (DDL) ---
    def create_table(self, transaction, table_name: str, schema: Dict[str, Any]):
        """
        创建一个新表。
        
        :param table_name: 表的唯一名称。
        :param schema: 表的模式，定义了列名和类型。
        """
        raise NotImplementedError

    # --- 数据操作 (CRUD) ---
    def insert_row(self, transaction, table_name: str, row: Tuple) -> Any:
        """
        (C)reate: 向表中插入一条新记录。
        
        :param table_name: 要插入的表名。
        :param row: 一行数据，以元组(tuple)形式表示。
        :return: 一个唯一标识该新行的行ID (row_id)。
        """
        raise NotImplementedError

    def scan(self, transaction, table_name: str) -> Iterator[Tuple[Any, Tuple]]:
        """
        (R)ead: 以全表扫描的方式读取所有记录。
        
        返回的迭代器需要包含每行的唯一标识符(row_id)，以便上层的Update/Delete算子使用。
        
        :param table_name: 要扫描的表名。
        :return: 一个迭代器。每次迭代返回一个元组 (row_id, row_data)。
        """
        raise NotImplementedError

    def update_row(self, transaction, table_name: str, row_id: Any, new_row: Tuple):
        """
        (U)pdate: 更新一条指定的记录。
        
        :param table_name: 表名。
        :param row_id: 用于唯一标识待更新行的值。
        :param new_row: 替换原始行的完整新数据。
        """
        raise NotImplementedError

    def delete_row(self, transaction, table_name: str, row_id: Any):
        """
        (D)elete: 删除一条指定的记录。
        
        :param table_name: 表名。
        :param row_id: 用于唯一标识待删除行的值。
        """
        raise NotImplementedError
