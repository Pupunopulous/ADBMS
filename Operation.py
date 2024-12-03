from enum import Enum

class OperationType(Enum):
    READ = "READ"
    WRITE = "WRITE"

class Operation:
    def __init__(self, ts: int, tid: int, vid: int, otype: OperationType, value: int):
        self.timestamp = ts
        self.transaction_id = tid
        self.variable_id = vid
        self.type = otype
        self.value = value

    def get_timestamp(self) -> int:
        return self.timestamp

    def get_transaction_id(self) -> int:
        return self.transaction_id

    def get_variable_id(self) -> int:
        return self.variable_id

    def get_type(self) -> OperationType:
        return self.type

    def get_value(self) -> int:
        return self.value

    def set_value(self, value: int) -> None:
        if self.type == OperationType.READ:
            self.value = value