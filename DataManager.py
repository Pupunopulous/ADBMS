from typing import Dict, List

from Variable import Variable
from LockManager import LockManager
from Transaction import TransactionType
from Operation import Operation, OperationType

class DataManager:
    VARIABLE_COUNT = 20

    def __init__(self, sid: int):
        self.id = sid
        self.active = True
        self.variables: Dict[int, Variable] = {}
        self.lock_managers: Dict[int, LockManager] = {}

        for i in range(1, DataManager.VARIABLE_COUNT + 1):
            if i % 2 == 0 or (i % 10 + 1) == sid:
                self.variables[i] = Variable(i)
                self.lock_managers[i] = LockManager(i)

    def get_id(self) -> int:
        return self.id

    def is_active(self) -> bool:
        return self.active

    def contains_variable(self, vid: int) -> bool:
        return vid in self.variables

    def can_read(self, ttype: TransactionType, operation: Operation) -> bool:
        if not self.is_active or operation.get_variable_id() not in self.variables:
            return False
        variable = self.variables[operation.get_variable_id()]
        if not variable.is_readable():
            return False
        if ttype == TransactionType.READ_WRITE:
            return self.lock_managers[operation.get_variable_id()].can_acquire_lock(
                operation.get_type(), operation.get_transaction_id()
            )
        return True

    def read(self, ttype: TransactionType, ts: int, operation: Operation) -> int:
        if operation.get_type() == OperationType.READ and self.can_read(ttype, operation):
            variable = self.variables[operation.get_variable_id()]
            if ttype == TransactionType.READ_ONLY:
                return self.read_by_read_only_transaction(ts, operation, variable)
            else:
                self.lock_managers[operation.get_variable_id()].lock(
                    operation.get_type(), operation.get_transaction_id(), operation.get_variable_id()
                )
                return self.read_by_read_write_transaction(operation, variable)
        return 0

    def read_by_read_only_transaction(self, ts: int, operation: Operation, variable: Variable) -> int:
        value = variable.get_last_committed_value_before(ts)
        operation.set_value(value)
        return value

    def read_by_read_write_transaction(self, operation: Operation, variable: Variable) -> int:
        if variable.get_transaction_id_to_commit() == operation.get_transaction_id():
            value = variable.get_value_to_commit()
            operation.set_value(value)
            return value
        else:
            value = variable.get_last_committed_value()
            operation.set_value(value)
            return value

    def can_write(self, ttype: TransactionType, operation: Operation) -> bool:
        if not self.is_active or ttype == TransactionType.READ_ONLY or operation.get_variable_id() not in self.variables:
            return False
        return self.lock_managers[operation.get_variable_id()].can_acquire_lock(
            operation.get_type(), operation.get_transaction_id()
        )

    def write(self, ttype: TransactionType, operation: Operation) -> None:
        if operation.get_type() == OperationType.WRITE and self.can_write(ttype, operation):
            lock_manager = self.lock_managers[operation.get_variable_id()]
            lock_manager.lock(operation.get_type(), operation.get_transaction_id(), operation.get_variable_id())
            variable = self.variables[operation.get_variable_id()]
            variable.set_value_to_commit(operation.get_value())
            variable.set_transaction_id_to_commit(operation.get_transaction_id())

    def get_lock_holders(self, vid: int) -> List[int]:
        if vid in self.lock_managers:
            return self.lock_managers[vid].get_lock_holders()
        return []

    def abort(self, tid: int) -> None:
        for lock_manager in self.lock_managers.values():
            lock_manager.unlock(tid)

    def commit(self, tid: int, ts: int) -> None:
        for lock_manager in self.lock_managers.values():
            if lock_manager.is_write_locked_by(tid):
                self.variables[lock_manager.get_variable_id()].commit(ts)
            lock_manager.unlock(tid)

    def dump(self) -> None:
        variables = [str(self.variables[i]) for i in range(1, DataManager.VARIABLE_COUNT + 1) if i in self.variables]
        print(f"site {self.id} - {', '.join(variables)}")

    def fail(self) -> None:
        self.active = False
        for variable in self.variables.values():
            variable.fail()
        for lock_manager in self.lock_managers.values():
            lock_manager.unlock_all()

    def recover(self) -> None:
        self.active = True
        for variable in self.variables.values():
            variable.recover()