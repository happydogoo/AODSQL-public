# transaction_manager.py

import threading
import time
from typing import Dict, List, Optional, Set
from contextlib import contextmanager

from loguru import logger
from .transaction import Transaction, IsolationLevel, TransactionState
from .lock_manager import LockManager, DeadlockError, ResourceID,LockMode
from .log_manager import (
    LogManager, CommitLogRecord, AbortLogRecord, NULL_LSN, 
    LogType, CompensationLogRecord, UpdateLogRecord
)

class TransactionManager:
    """负责事务的创建、提交和中止，是所有组件的协调者"""
    
    def __init__(self, lock_manager: LockManager, log_manager: LogManager,start_txn_id: int = 1):
        self.lock_manager = lock_manager
        self.log_manager = log_manager
        self._next_transaction_id = start_txn_id
        self._transaction_table: Dict[int, Transaction] = {}
        # 死锁检测间隔（秒）
        self._deadlock_detection_interval = 1.0
        self._txn_map = {}
        self._lock = threading.Lock()
        # 启动死锁检测线程
        self._deadlock_detector_thread = threading.Thread(target=self._deadlock_detector, daemon=True)
        self._deadlock_detector_thread.start()

    def begin(self, isolation_level: IsolationLevel = IsolationLevel.REPEATABLE_READ) -> Transaction:
        """开始一个新事务"""
        with self._lock:
            txn_id = self._next_transaction_id
            self._next_transaction_id += 1
            transaction = Transaction(txn_id, isolation_level)
            self._transaction_table[txn_id] = transaction

            logger.info(f"开始事务 {txn_id}，隔离级别: {isolation_level.name}")

            return transaction

    def commit(self, transaction: Transaction) -> bool:
        """提交一个事务，确保持久性"""
        if transaction.state != TransactionState.ACTIVE:

            raise Exception(f"事务 {transaction.id} 状态异常，无法提交。当前状态: {transaction.state.name}")
        
        try:
            logger.info(f"开始提交事务 {transaction.id}")
            transaction.set_state(TransactionState.COMMITTING)
            
            # 写入提交日志记录
            commit_record = CommitLogRecord(transaction.id)
            commit_lsn = self.log_manager.append(transaction, commit_record)
            
            # 强制刷盘确保持久性
            self.log_manager.flush_to_lsn(commit_lsn)
            
            # 更新事务状态
            transaction.set_state(TransactionState.COMMITTED)
            
            # 释放所有锁
            self.lock_manager.release_all(transaction)
            
            # 从活跃事务表中移除
            with self._lock:
                if transaction.id in self._transaction_table:
                    del self._transaction_table[transaction.id]
            
            logger.info(f"事务 {transaction.id} 提交成功")
            return True
            
        except Exception as e:
            logger.error(f"事务 {transaction.id} 提交失败: {e}")
            self.abort(transaction)
            return False

    def abort(self, transaction: Transaction) -> bool:
        """中止一个事务，保证原子性。此方法必须保证能释放锁。"""
        if transaction.state in {TransactionState.COMMITTED, TransactionState.ABORTED}:
            logger.info(f"事务 {transaction.id} 已结束，无需重复中止")
            return True

        logger.info(f"开始中止事务 {transaction.id}")
        transaction.set_state(TransactionState.ABORTING)
        try:
            # --- 关键修正点：将回滚操作放入 try 块 ---
            # 确保与事务相关的日志都已刷盘
            self.log_manager.flush_to_lsn(transaction.last_lsn)
            # 执行回滚
            self._rollback(transaction)
        except Exception as e:
            # 即使回滚失败，也要记录下来，并继续执行后续的清理
            logger.error(f"事务 {transaction.id} 在回滚阶段发生严重错误: {e}")
        finally:
            # --- 关键修正点：将资源释放放入 finally 块 ---
            # 无论回滚是否成功，都必须执行以下清理步骤
            # 写入中止日志记录
            abort_record = AbortLogRecord(transaction.id)
            self.log_manager.append(transaction, abort_record)
            # 更新最终状态
            transaction.set_state(TransactionState.ABORTED)
            # **必须保证释放所有锁**
            self.lock_manager.release_all(transaction)
            # 从活跃事务表中移除
            with self._lock:
                if transaction.id in self._transaction_table:
                    del self._transaction_table[transaction.id]
            logger.info(f"事务 {transaction.id} 清理完成，已中止。")
            return True

    

    def get_transaction(self, transaction_id: int) -> Optional[Transaction]:
        """根据事务ID获取事务对象"""
        with self._lock:

            return self._transaction_table.get(transaction_id)

    def get_active_transactions(self) -> List[Transaction]:
        """获取所有活跃的事务"""
        with self._lock:
            return [txn for txn in self._transaction_table.values() 
                   if txn.state == TransactionState.ACTIVE]

    def get_transaction_count(self) -> int:
        """获取当前活跃事务数量"""
        with self._lock:
            return len(self._transaction_table)

    def set_transaction_timeout(self, timeout_seconds: int):
        """设置事务超时时间"""
        self._transaction_timeout = timeout_seconds

    def _deadlock_detector(self):
        """死锁检测线程"""
        while True:
            try:
                time.sleep(self._deadlock_detection_interval)
                
                # 检查超时事务
                current_time = time.time()
                with self._lock:
                    timeout_txns = []
                    for txn in self._transaction_table.values():
                        if (txn.state == TransactionState.ACTIVE and 
                            hasattr(txn, '_start_time') and 
                            current_time - txn._start_time > self._transaction_timeout):
                            timeout_txns.append(txn)
                
                # 中止超时事务
                for txn in timeout_txns:
                    logger.info(f"事务 {txn.id} 超时，自动中止")
                    self.abort(txn)
                    
            except Exception as e:
                logger.error(f"死锁检测线程异常: {e}")

    @contextmanager
    def transaction(self, isolation_level: IsolationLevel = IsolationLevel.REPEATABLE_READ):
        """事务上下文管理器，支持 with 语句"""
        txn = self.begin(isolation_level)
        txn._start_time = time.time()  # 记录开始时间用于超时检测
        
        try:
            yield txn
            self.commit(txn)
        except Exception as e:
            logger.info(f"事务 {txn.id} 发生异常，自动回滚: {e}")
            self.abort(txn)
            raise

    def acquire_lock(self, transaction: Transaction, lock_mode: LockMode, resource_id: ResourceID):
        """为事务获取锁"""
        if transaction.state != TransactionState.ACTIVE:
            raise Exception(f"事务 {transaction.id} 不是活跃状态，无法获取锁")
        
        try:
            self.lock_manager.acquire(transaction, lock_mode, resource_id)
            logger.info(f"事务 {transaction.id} 成功获取 {lock_mode.name} 锁: {resource_id}")
        except DeadlockError as e:
            logger.info(f"事务 {transaction.id} 发生死锁，自动中止")
            self.abort(transaction)
            raise

    def release_lock(self, transaction: Transaction, resource_id: ResourceID):
        """释放事务的特定锁"""
        if transaction.state not in {TransactionState.ACTIVE, TransactionState.COMMITTING, TransactionState.ABORTING}:
            return
        
        # 注意：在严格两阶段封锁(S2PL)中，锁只能在事务结束时释放
        # 这里提供接口但不实际释放，除非事务结束
        logger.info(f"事务 {transaction.id} 请求释放锁: {resource_id} (S2PL模式下将在事务结束时释放)")

    def log_update(self, transaction: Transaction, resource_id: ResourceID, 
                   before_image: bytes, after_image: bytes) -> int:
        """记录更新操作的日志"""
        if transaction.state != TransactionState.ACTIVE:
            raise Exception(f"事务 {transaction.id} 不是活跃状态，无法记录日志")
        
        update_record = UpdateLogRecord(transaction.id, resource_id, before_image, after_image)
        lsn = self.log_manager.append(transaction, update_record)
        logger.info(f"事务 {transaction.id} 记录更新日志，LSN: {lsn}")
        return lsn

    def force_log_flush(self, transaction: Transaction):
        """强制刷盘事务的日志"""
        if transaction.last_lsn != NULL_LSN:
            self.log_manager.flush_to_lsn(transaction.last_lsn)
            logger.info(f"强制刷盘事务 {transaction.id} 的日志到 LSN {transaction.last_lsn}")

    def get_transaction_status(self, transaction_id: int) -> Optional[str]:
        """获取事务状态信息"""
        txn = self.get_transaction(transaction_id)
        if txn is None:
            return None
        
        status = {
            'id': txn.id,
            'state': txn.state.name,
            'isolation_level': txn.isolation_level.name,
            'last_lsn': txn.last_lsn,
            'held_locks_count': len(txn.get_held_locks())
        }
        
        if hasattr(txn, '_start_time'):
            status['duration'] = time.time() - txn._start_time
        
        return status

    def shutdown(self):
        """关闭事务管理器，中止所有活跃事务"""
        logger.info("正在关闭事务管理器...")
        
        with self._lock:
            active_txns = list(self._transaction_table.values())
        
        for txn in active_txns:
            if txn.state == TransactionState.ACTIVE:
                logger.info(f"中止活跃事务 {txn.id}")
                self.abort(txn)
        
        logger.info("事务管理器已关闭")

    def _rollback(self, transaction: Transaction):
        """
        通过反向遍历日志链来执行回滚，并为每次撤销写入补偿日志(CLR)。
        """
        current_undo_lsn = transaction.last_lsn
        while current_undo_lsn != NULL_LSN:
            
            record = self.log_manager.read_log_record_by_lsn(current_undo_lsn)
            if record is None:
                raise Exception(f"无法找到 LSN={current_undo_lsn} 的日志记录，回滚失败")
            if record.log_type in {LogType.UPDATE, LogType.INSERT, LogType.DELETE}:
                logger.info(f"  [Rollback] 正在撤销 LSN {record.lsn}...")
                undo_info = record.get_undo_info()
                clr = CompensationLogRecord(
                    transaction.id,
                    record.prev_lsn,
                    record.log_type,
                    undo_info
                )
                clr_lsn = self.log_manager.append(transaction, clr)
                self.log_manager.flush_to_lsn(clr_lsn)
                clr.redo(self.log_manager._storage_engine, transaction=transaction)
            if record.log_type == LogType.CLR:
                current_undo_lsn = record.undo_next_lsn
            else:
                current_undo_lsn = record.prev_lsn
