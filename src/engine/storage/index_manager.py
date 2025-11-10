from src.engine.storage.btree_manager import BTreeManager
from src.engine.transaction.transaction import Transaction

class IndexManager:
    def __init__(self, table_info, data_buffer_pool, index_buffer_pools, catalog_manager):
        self.table_info = table_info
        self.data_buffer_pool = data_buffer_pool  # 仅当前表的数据BufferPool
        self.index_buffer_pools = index_buffer_pools  # dict: index_name -> BufferPool
        self.catalog_manager = catalog_manager
        self.table_name = table_info.table_name
        

    def insert_entry(self, transaction: Transaction, row_tuple, row_id, lsn_map: dict):
        for index_name, idx_info in self.table_info.indexes.items():
            key = self._get_key_for_index(row_tuple, idx_info)
            bp = self.index_buffer_pools[index_name]
            bptm = BTreeManager(bp, self.catalog_manager, self.table_name, index_name, idx_info.key_col_types)
            bptm.insert(transaction, key, row_id, lsn_map[index_name])

    def delete_entry(self, transaction: Transaction, row_tuple, row_id, lsn_map: dict):
        for index_name, idx_info in self.table_info.indexes.items():
            key = self._get_key_for_index(row_tuple, idx_info)
            bp = self.index_buffer_pools[index_name]
            bptm = BTreeManager(bp, self.catalog_manager, self.table_name, index_name, idx_info.key_col_types)
            bptm.delete(transaction, key, row_id, lsn_map[index_name])

    def update_entries(self, transaction: Transaction, old_row_tuple, new_row_tuple, row_id, lsn_map: dict):
        for index_name, idx_info in self.table_info.indexes.items():
            old_key = self._get_key_for_index(old_row_tuple, idx_info)
            new_key = self._get_key_for_index(new_row_tuple, idx_info)
            if old_key != new_key:
                bp = self.index_buffer_pools[index_name]
                bptm = BTreeManager(bp, self.catalog_manager, self.table_name, index_name, idx_info.key_col_types)
                bptm.delete(transaction, old_key, row_id, lsn_map.get(f"{index_name}_delete", lsn_map[index_name]))
                bptm.insert(transaction, new_key, row_id, lsn_map.get(f"{index_name}_insert", lsn_map[index_name]))

    def _get_key_for_index(self, row_tuple, idx_info) -> tuple:
        key_indices = [i for i, col in enumerate(self.table_info.columns)
                       if (getattr(col, 'column_name', None) in idx_info.column_names)
                       or (isinstance(col, tuple) and col[0] in idx_info.column_names)]
        return tuple(row_tuple[i] for i in key_indices) 