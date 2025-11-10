import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import pytest
from src.storage.btreepage import BTreePageBase, BTreeLeafPage, BTreeInternalPage
from src.engine.transaction.transaction import Transaction, IsolationLevel

def make_transaction():
    return Transaction(1, IsolationLevel.SERIALIZABLE)

@pytest.fixture
def dummy_transaction():
    return make_transaction()

def test_btree_page_base_lsn_persistence():
    page = BTreePageBase(page_id=1)
    page.set_page_lsn(888)
    data = page.data
    page2 = BTreePageBase(page_id=1, data=data)
    assert page2.get_page_lsn() == 888

def test_leaf_page_modifications_set_lsn(dummy_transaction):
    page = BTreeLeafPage(page_id=2)
    page.insert(dummy_transaction, (10,), (1,1), [1], lsn=101)
    assert page.get_page_lsn() == 101
    page.delete(dummy_transaction, (10,), (1,1), [1], lsn=102)
    assert page.get_page_lsn() == 102

def test_leaf_page_split_sets_lsn_on_both_pages(dummy_transaction):
    page = BTreeLeafPage(page_id=3)
    # 填充到接近满
    for i in range(10):
        page.insert(dummy_transaction, (i,), (i, i), [1], lsn=150)
    right_page = BTreeLeafPage(page_id=4)
    up_key = page.split(dummy_transaction, right_page, lsn=200)
    assert page.get_page_lsn() == 200
    assert right_page.get_page_lsn() == 200

def test_internal_page_modifications_set_lsn(dummy_transaction):
    page = BTreeInternalPage(page_id=5)
    # 插入
    page.insert(dummy_transaction, (1,), 100, [1], lsn=201)
    assert page.get_page_lsn() == 201
    # 删除
    page.delete(dummy_transaction, (1,), [1], lsn=202)
    assert page.get_page_lsn() == 202

def test_internal_page_split_sets_lsn_on_both_pages(dummy_transaction):
    page = BTreeInternalPage(page_id=6)
    # 填充到接近满
    for i in range(10):
        page.insert(dummy_transaction, (i,), i+100, [1], lsn=250)
    up_key, new_right = page.split(dummy_transaction, new_page_id=7, lsn=300)
    assert page.get_page_lsn() == 300
    assert new_right.get_page_lsn() == 300 