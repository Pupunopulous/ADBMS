from enum import Enum

class LockType(Enum):
    READ_LOCK = "READ_LOCK"
    WRITE_LOCK = "WRITE_LOCK"

class Lock:
    def __init__(self, tid: int, vid: int, ltype: LockType):
        self.transaction_id = tid
        self.variable_id = vid
        self.type = ltype

    def get_transaction_id(self) -> int:
        return self.transaction_id

    def get_variable_id(self) -> int:
        return self.variable_id

    def get_type(self) -> LockType:
        return self.type

    def promote_lock_type(self) -> None:
        self.type = LockType.WRITE_LOCK