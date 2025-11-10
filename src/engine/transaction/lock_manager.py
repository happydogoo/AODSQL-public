# lock_manager.py (Corrected for Hierarchical Locking Bug)
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import threading
from enum import Enum, auto
from typing import NamedTuple, Dict, List, Set
from collections import defaultdict

# --- 预定义的数据结构和异常 ---

class DeadlockError(Exception):
    """当检测到死锁时抛出"""
    def __init__(self, message: str, transaction):
        super().__init__(message)
        self.transaction = transaction

class LockMode(Enum):
    SHARED = auto()
    EXCLUSIVE = auto()
    INTENTION_SHARED = auto()
    INTENTION_EXCLUSIVE = auto()
    SHARED_INTENTION_EXCLUSIVE = auto()

class ResourceID(NamedTuple):
    table_name: str
    page_id: int = None
    record_id: int = None

class LockRequest:
    def __init__(self, transaction, lock_mode: LockMode):
        self.transaction = transaction
        self.lock_mode = lock_mode
        self.granted = threading.Event()

class LockRequestQueue:
    def __init__(self):
        self.queue: List[LockRequest] = []
        self.lock = threading.Lock()
        # granted_locks: txn_id -> LockMode
        self.granted_locks: Dict[int, LockMode] = {}

    def is_compatible(self, lock_mode: LockMode, except_txn: int = None) -> bool:
        for txn_id, mode in self.granted_locks.items():
            if except_txn is not None and txn_id == except_txn:
                continue
            if not LockRequestQueue.lock_compatible(mode, lock_mode):
                return False
        return True

    @staticmethod
    def lock_compatible(held: LockMode, request: LockMode) -> bool:
        # S与S/IS兼容，X与任何都不兼容，IX与S不兼容
        if held == LockMode.EXCLUSIVE or request == LockMode.EXCLUSIVE:
            return False
        if held == LockMode.SHARED and request in (LockMode.SHARED, LockMode.INTENTION_SHARED):
            return True
        if held == LockMode.INTENTION_SHARED and request in (LockMode.SHARED, LockMode.INTENTION_SHARED, LockMode.INTENTION_EXCLUSIVE):
            return True
        if held == LockMode.INTENTION_EXCLUSIVE and request == LockMode.INTENTION_EXCLUSIVE:
            return True
        return False

    def add_request(self, request: LockRequest):
        self.queue.append(request)

    def grant_locks(self):
        i = 0
        while i < len(self.queue):
            req = self.queue[i]
            if self.is_compatible(req.lock_mode, except_txn=req.transaction.id):
                self.granted_locks[req.transaction.id] = req.lock_mode
                req.granted.set()
                self.queue.pop(i)
            else:
                i += 1

    def release(self, txn_id: int):
        if txn_id in self.granted_locks:
            del self.granted_locks[txn_id]
        self.grant_locks()

class LockManager:
    def __init__(self):
        self._lock_table: Dict[ResourceID, LockRequestQueue] = {}
        self._global_lock = threading.Lock()
        self._waits_for_graph: Dict[int, Set[int]] = {}

    def acquire(self, transaction, lock_mode: LockMode, resource_id: ResourceID):
        req = LockRequest(transaction, lock_mode)
        txn_id = transaction.id
        
        # 1. 递归加父级锁（不持有全局锁）
        
        # BUG FIX START: 修正父级意向锁的判断逻辑
        # 正确逻辑：如果当前请求的是任何带有“排他”性质的锁 (X, IX, SIX)，
        # 那么父级就必须加 IX 锁。否则加 IS 锁。
        parent_intention_mode = LockMode.INTENTION_SHARED
        if lock_mode in (LockMode.EXCLUSIVE, LockMode.INTENTION_EXCLUSIVE, LockMode.SHARED_INTENTION_EXCLUSIVE):
            parent_intention_mode = LockMode.INTENTION_EXCLUSIVE
        
        if resource_id.record_id is not None:
            page_res = ResourceID(resource_id.table_name, resource_id.page_id)
            self.acquire(transaction, parent_intention_mode, page_res)
        # 使用 elif 避免在同一次调用中对 page 和 table 都执行父级锁请求
        elif resource_id.page_id is not None: 
            table_res = ResourceID(resource_id.table_name)
            self.acquire(transaction, parent_intention_mode, table_res)
        # BUG FIX END

        # 2. 只在操作共享数据结构时加锁
        with self._global_lock:
            queue = self._lock_table.setdefault(resource_id, LockRequestQueue())
            if (txn_id in queue.granted_locks and queue.granted_locks[txn_id] == lock_mode) or (queue.is_compatible(lock_mode, except_txn=txn_id) and not queue.queue):
                queue.granted_locks[txn_id] = lock_mode
                req.granted.set()
            else:
                queue.add_request(req)
                holders = set(queue.granted_locks.keys())
                self._waits_for_graph.setdefault(txn_id, set()).update(holders)
                if self._detect_deadlock(txn_id, set()):
                    queue.queue.remove(req)
                    self._waits_for_graph[txn_id].difference_update(holders)
                    raise DeadlockError(f"检测到死锁，事务{txn_id}被选为牺牲者", transaction)
        req.granted.wait()
        transaction.add_held_lock(resource_id)

    def release_all(self, transaction):
        txn_id = transaction.id
        with self._global_lock:
            for resource_id in transaction.get_held_locks():
                queue = self._lock_table.get(resource_id)
                if queue:
                    queue.release(txn_id)
            if txn_id in self._waits_for_graph:
                del self._waits_for_graph[txn_id]
            for waiters in self._waits_for_graph.values():
                waiters.discard(txn_id)
        transaction.remove_all_held_locks()

    def _detect_deadlock(self, start_txn: int, visited: Set[int]) -> bool:
        if start_txn in visited:
            return True
        visited.add(start_txn)
        for next_txn in self._waits_for_graph.get(start_txn, set()):
            if self._detect_deadlock(next_txn, visited):
                return True
        visited.remove(start_txn)
        return False