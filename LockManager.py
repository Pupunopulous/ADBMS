from typing import List

from Lock import Lock, LockType
from Operation import OperationType


class LockManager:
    def __init__(self, vid: int):
        self.variable_id = vid
        self.locks: List[Lock] = []

    def get_variable_id(self) -> int:
        return self.variable_id

    def can_acquire_lock(self, operation_type: OperationType, tid: int) -> bool:
        if operation_type == OperationType.READ:
            return self.can_acquire_read_lock(tid)
        else:
            return self.can_acquire_write_lock(tid)

    def can_acquire_read_lock(self, tid: int) -> bool:
        write_lock = self.get_write_lock()
        return write_lock is None or write_lock.get_transaction_id() == tid

    def can_acquire_write_lock(self, tid: int) -> bool:
        if len(self.locks) > 1:
            return False
        if len(self.locks) == 1 and self.get_lock(tid) is None:
            return False
        return True

    def lock(self, operation_type: OperationType, tid: int, vid: int) -> None:
        if operation_type == OperationType.READ:
            self.lock_for_read(tid, vid)
        else:
            self.lock_for_write(tid, vid)

    def lock_for_read(self, tid: int, vid: int) -> None:
        if self.can_acquire_read_lock(tid):
            transaction_lock = self.get_lock(tid)
            if transaction_lock is None:
                self.locks.append(Lock(tid, vid, LockType.READ_LOCK))

    def lock_for_write(self, tid: int, vid: int) -> None:
        if self.can_acquire_write_lock(tid):
            transaction_lock = self.get_lock(tid)
            if transaction_lock is None:
                self.locks.append(Lock(tid, vid, LockType.WRITE_LOCK))
            else:
                transaction_lock.promote_lock_type()

    def unlock(self, tid: int) -> None:
        self.locks = [l for l in self.locks if l.get_transaction_id() != tid]

    def unlock_all(self) -> None:
        self.locks.clear()

    def is_write_locked_by(self, tid: int) -> bool:
        write_lock = self.get_write_lock()
        return write_lock is not None and write_lock.get_transaction_id() == tid

    def get_lock_holders(self) -> List[int]:
        return [lock.get_transaction_id() for lock in self.locks]

    def get_write_lock(self) -> Lock:
        for lock in self.locks:
            if lock.get_type() == LockType.WRITE_LOCK:
                return lock
        return None

    def get_lock(self, tid: int) -> Lock:
        for lock in self.locks:
            if lock.get_transaction_id() == tid:
                return lock
        return None