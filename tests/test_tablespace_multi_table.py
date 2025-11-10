import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.storage import TablespaceManager
from src.storage import BufferPool

def test_delete_all_table_pages():
    print('\n===== 测试批量删除表的所有节点页（仅缓存） =====')
    test_table = 'test_delete_all'
    fname = f'{test_table}.ibd'
    if os.path.exists(fname):
        os.remove(fname)
    tsm = TablespaceManager(fname, page_size=256)
    buf = BufferPool(tsm, buffer_size=8)
    # 分配5个页，构成链表
    page_ids = []
    for i in range(5):
        page = buf.new_page()
        page_ids.append(page.page_id)
        if i > 0:
            prev = buf.get_page(page_ids[i-1])
            prev.next_page_id = page.page_id
            print(f'  设置 page {prev.page_id} 的 next_page_id = {page.page_id}')
            buf.unpin_page(prev.page_id, is_dirty=True)
        print(f'  分配页: id={page.page_id}, next_page_id={page.next_page_id}')
        buf.unpin_page(page.page_id, is_dirty=True)
    print(f'  分配页链表: {page_ids}')
    print(f'  删除前缓存内容: {list(buf.cache.keys())}')
    # 删除所有节点页
    print('  开始批量删除...')
    try:
        buf.delete_table_pages(page_ids[0])
        print('  已批量删除所有节点页')
    except Exception as e:
        print(f'  删除过程中发生异常: {e}')
    print(f'  删除后缓存内容: {list(buf.cache.keys())}')
    tsm.close()
    if os.path.exists(fname):
        os.remove(fname)
    print('===== 节点页批量删除测试结束 =====')

if __name__ == '__main__':
    test_delete_all_table_pages() 