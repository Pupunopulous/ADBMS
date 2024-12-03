from collections import OrderedDict

class Variable:
    def __init__(self, vid: int):
        self.id = vid
        self.value_to_commit = 10 * vid
        self.last_committed_value = 10 * vid
        self.committed_values = OrderedDict({0: self.last_committed_value})
        self.transaction_id_to_commit = None
        self.readable = True

    def get_id(self) -> int:
        return self.id

    def get_value_to_commit(self) -> int:
        return self.value_to_commit

    def get_transaction_id_to_commit(self) -> int:
        return self.transaction_id_to_commit

    def get_last_committed_value(self) -> int:
        return self.last_committed_value

    def get_last_committed_value_before(self, ts: int) -> int:
        committed_value = 0
        for timestamp, value in self.committed_values.items():
            if timestamp <= ts:
                committed_value = value
            else:
                break
        return committed_value

    def is_readable(self) -> bool:
        return self.readable

    def set_value_to_commit(self, value: int) -> None:
        self.value_to_commit = value

    def set_transaction_id_to_commit(self, tid: int) -> None:
        self.transaction_id_to_commit = tid

    def fail(self) -> None:
        self.readable = False

    def recover(self) -> None:
        if self.id % 2 != 0:
            self.readable = True

    def commit(self, ts: int) -> None:
        self.last_committed_value = self.value_to_commit
        self.committed_values[ts] = self.last_committed_value
        self.readable = True

    def __str__(self) -> str:
        return f"x{self.id}: {self.last_committed_value}"