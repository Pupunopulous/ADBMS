from enum import Enum
from typing import Set

class TransactionType(Enum):
    READ_ONLY = "READ_ONLY"
    READ_WRITE = "READ_WRITE"

class Transaction:
    def __init__(self, tid: int, ts: int, ttype: TransactionType):
        self.id = tid
        self.timestamp = ts
        self.type = ttype
        self.blocked = False  # Changed attribute name
        self.aborted = False
        self.accessed_sites: Set[int] = set()

    def get_id(self) -> int:
        return self.id

    def get_timestamp(self) -> int:
        return self.timestamp

    def get_type(self) -> TransactionType:
        return self.type

    def is_blocked(self) -> bool:  # Changed method name
        return self.blocked

    def block(self) -> None:
        self.blocked = True

    def unblock(self) -> None:
        self.blocked = False

    def is_aborted(self) -> bool:
        return self.aborted

    def set_aborted(self) -> None:
        self.aborted = True

    def get_accessed_sites(self) -> Set[int]:
        return self.accessed_sites

    def add_accessed_site(self, sid: int) -> None:
        self.accessed_sites.add(sid)

    def has_accessed_site(self, sid: int) -> bool:
        return sid in self.accessed_sites
