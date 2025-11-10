# transaction.py

from enum import Enum, auto
from typing import Set, TYPE_CHECKING
# 使用 TYPE_CHECKING 来避免循环导入，同时为类型提示提供支持
if TYPE_CHECKING:
    from lock_manager import ResourceID
    
class IsolationLevel(Enum):
    """定义事务的隔离级别 (为未来扩展做准备)"""
    READ_COMMITTED = auto()
    REPEATABLE_READ = auto()
    SERIALIZABLE = auto()

class TransactionState(Enum):
    """定义事务的生命周期状态"""
    ACTIVE = auto()       # 活跃，正在执行
    COMMITTING = auto()   # 正在提交，已写入COMMIT日志但尚未释放锁
    COMMITTED = auto()    # 已提交
    ABORTING = auto()     # 正在中止，执行回滚操作
    ABORTED = auto()      # 已中止

class Transaction:
    """
    事务上下文对象，封装了单个事务的所有状态信息。
    这是一个被动的数据容器，其状态由 TransactionManager 驱动改变。
    """
    def __init__(self, transaction_id: int, isolation_level: IsolationLevel):
        self._id: int = transaction_id
        self._state: TransactionState = TransactionState.ACTIVE
        self._isolation_level: IsolationLevel = isolation_level
        
        # 该事务所持有的所有锁的资源ID (由 LockManager 维护)
        self._held_locks: Set['ResourceID'] = set()

        # 该事务写入的最后一条日志的 LSN (由 LogManager 维护)
        self._last_lsn: int = 0

    @property
    def id(self) -> int:
        """事务的唯一ID"""
        return self._id

    @property
    def state(self) -> TransactionState:
        """事务的当前状态"""
        return self._state

    def set_state(self, new_state: TransactionState):
        """由 TransactionManager 控制的状态转换"""
        self._state = new_state
        
    @property
    def isolation_level(self) -> IsolationLevel:
        """事务的隔离级别"""
        return self._isolation_level

    @property
    def last_lsn(self) -> int:
        """获取事务最后一条日志的LSN"""
        return self._last_lsn
        
    @last_lsn.setter
    def last_lsn(self, lsn: int):
        """设置事务最后一条日志的LSN"""
        self._last_lsn = lsn

    def add_held_lock(self, resource_id: 'ResourceID'):
        """登记一个该事务已持有的锁"""
        self._held_locks.add(resource_id)
        
    def remove_held_lock(self, resource_id: 'ResourceID'):
        """移除一个该事务已释放的锁"""
        self._held_locks.discard(resource_id)

    def get_held_locks(self) -> Set['ResourceID']:
        """获取该事务持有的所有锁的资源ID集合"""
        return self._held_locks
    
    def clear_held_locks(self):
        if hasattr(self, '_held_locks'):
            self._held_locks.clear()
    def remove_all_held_locks(self):
        """移除并清空所有持有的锁"""
        self._held_locks.clear()

    def __hash__(self):
        return hash(self._id)

    def __eq__(self, other):
        if isinstance(other, Transaction):
            return self._id == other._id
        return False