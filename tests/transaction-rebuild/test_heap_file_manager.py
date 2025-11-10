import pytest
from unittest.mock import MagicMock
from src.engine.storage.heap_file_manager import HeapFileManager

class DummyTransaction:
    pass

class DummyTableInfo:
    def __init__(self):
        self.root_page_id = None
        self.last_page_id = None
        self.table_name = 'test_table'

@pytest.fixture
def dummy_transaction():
    return DummyTransaction()

@pytest.fixture
def mock_page():
    page = MagicMock()
    page.is_full.return_value = False
    page.insert_record.return_value = 0
    page.key_count = 1
    return page

@pytest.fixture
def mock_buffer_pool(mock_page):
    bp = MagicMock()
    bp.get_page.return_value = mock_page
    bp.new_page.return_value = mock_page
    return bp

@pytest.fixture
def dummy_table_info():
    return DummyTableInfo()

def test_insert_record_on_empty_table(dummy_transaction, mock_buffer_pool, mock_page, dummy_table_info):
    dummy_table_info.last_page_id = None
    hfm = HeapFileManager(mock_buffer_pool, dummy_table_info, catalog_manager=None)
    hfm.insert_record(dummy_transaction, b'data', lsn=101)
    mock_buffer_pool.new_page.assert_called_once()
    mock_page.insert_record.assert_called_once_with(dummy_transaction, b'data', 101)
    mock_buffer_pool.unpin_page.assert_called_once_with(mock_page.page_id, is_dirty=True)

def test_insert_record_on_full_page(dummy_transaction, mock_buffer_pool, mock_page, dummy_table_info):
    dummy_table_info.last_page_id = 1
    mock_page.is_full.return_value = True
    hfm = HeapFileManager(mock_buffer_pool, dummy_table_info, catalog_manager=None)
    hfm.insert_record(dummy_transaction, b'data', lsn=102)
    mock_buffer_pool.get_page.assert_called_once_with(1)
    assert mock_buffer_pool.unpin_page.call_args_list[0][1]['is_dirty'] is False
    mock_buffer_pool.new_page.assert_called_once()
    mock_page.insert_record.assert_called_with(dummy_transaction, b'data', 102)
    assert mock_buffer_pool.unpin_page.call_args_list[-1][1]['is_dirty'] is True

@pytest.mark.parametrize('op, page_method, args, lsn', [
    ('update', 'update_record', (0, b'new_data'), 201),
    ('delete', 'mark_as_deleted', (0, 8), 301),
])
def test_update_and_delete_pass_context_correctly(dummy_transaction, mock_buffer_pool, mock_page, dummy_table_info, op, page_method, args, lsn):
    dummy_table_info.last_page_id = 1
    hfm = HeapFileManager(mock_buffer_pool, dummy_table_info, catalog_manager=None)
    row_id = (1, 0)
    if op == 'update':
        hfm.update_record(dummy_transaction, row_id, b'new_data', lsn=lsn)
        getattr(mock_page, page_method).assert_called_once_with(dummy_transaction, 0, b'new_data', lsn)
    else:
        hfm.delete_record(dummy_transaction, row_id, 8, lsn=lsn)
        getattr(mock_page, page_method).assert_called_once_with(dummy_transaction, 0, 8, lsn)
    mock_buffer_pool.unpin_page.assert_called_with(1, is_dirty=True) 