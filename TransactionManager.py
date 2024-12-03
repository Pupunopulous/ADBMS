from collections import defaultdict
from typing import Dict, List

from DataManager import DataManager
from Transaction import Transaction, TransactionType
from Operation import Operation, OperationType

class TransactionManager:
    SITE_COUNT = 10

    def __init__(self):
        self.sites = {i: DataManager(i) for i in range(1, TransactionManager.SITE_COUNT + 1)}
        self.transactions: Dict[int, Transaction] = {}
        self.waiting_operations: List[Operation] = []
        self.waits_for_graph: Dict[int, set] = defaultdict(set)

    def begin(self, tid: int, ts: int) -> None:
        if tid not in self.transactions:
            self.transactions[tid] = Transaction(tid, ts, TransactionType.READ_WRITE)
            print(f"T{tid} begins")

    def begin_ro(self, tid: int, ts: int) -> None:
        if tid not in self.transactions:
            self.transactions[tid] = Transaction(tid, ts, TransactionType.READ_ONLY)
            print(f"T{tid} begins and is read-only")

    def end(self, tid: int, ts: int) -> None:
        if tid in self.transactions:
            transaction = self.transactions[tid]
            if transaction.is_aborted():
                print(f"T{tid} aborts due to previous access of a down site")
                self.abort(tid)
            else:
                if transaction.get_type() == TransactionType.READ_WRITE:
                    for site_id in transaction.get_accessed_sites():
                        self.sites[site_id].commit(tid, ts)
                print(f"T{tid} commits")
                del self.transactions[tid]
                self.waits_for_graph.pop(tid, None)
                self.retry()

    def read(self, tid: int, vid: int, ts: int) -> None:
        if tid not in self.transactions:
            return
        transaction = self.transactions[tid]
        operation = Operation(ts, tid, vid, OperationType.READ, 0)
        for site_id, site in self.sites.items():
            if site.can_read(transaction.get_type(), operation):
                value = site.read(transaction.get_type(), transaction.get_timestamp(), operation)
                transaction.add_accessed_site(site_id)
                transaction.unblock()
                print(f"T{tid} reads x{vid}: {value}")
                return
        self.waiting_operations.append(operation)
        transaction.block()
        print(f"T{tid} blocked")
        self.detect_deadlock(tid)

    def write(self, tid: int, vid: int, value: int, ts: int) -> None:
        if tid not in self.transactions:
            return
        transaction = self.transactions[tid]
        operation = Operation(ts, tid, vid, OperationType.WRITE, value)
        can_write = all(
            site.is_active() and site.can_write(transaction.get_type(), operation)
            for site in self.sites.values()
        )
        if can_write:
            for site in self.sites.values():
                if site.is_active() and site.contains_variable(vid):
                    site.write(transaction.get_type(), operation)
                    transaction.add_accessed_site(site.get_id())
            transaction.unblock()
            print(f"T{tid} writes x{vid}: {value}")
        else:
            self.waiting_operations.append(operation)
            transaction.block()
            print(f"T{tid} blocked")
            self.detect_deadlock(tid)

    def dump(self) -> None:
        for site in self.sites.values():
            site.dump()

    def fail(self, sid: int) -> None:
        if sid in self.sites:
            self.sites[sid].fail()
            for transaction in self.transactions.values():
                if transaction.has_accessed_site(sid):
                    transaction.set_aborted()
            print(f"site {sid} fails")

    def recover(self, sid: int) -> None:
        if sid in self.sites:
            self.sites[sid].recover()
            print(f"site {sid} recovers")
            self.retry()

    def abort(self, tid: int) -> None:
        for site in self.sites.values():
            site.abort(tid)
        self.transactions.pop(tid, None)
        self.retry()

    def retry(self) -> None:
        for operation in self.waiting_operations[:]:
            tid = operation.get_transaction_id()
            if tid not in self.transactions:
                continue
            if operation.get_type() == OperationType.READ:
                self.read(tid, operation.get_variable_id(), operation.get_timestamp())
            else:
                self.write(tid, operation.get_variable_id(), operation.get_value(), operation.get_timestamp())
            if not self.transactions[tid].is_blocked():
                self.waiting_operations.remove(operation)

    def detect_deadlock(self, tid: int) -> None:
        visited = set()
        cycle = self._detect_cycle(tid, visited, [])
        if cycle:
            youngest_tid = max(cycle, key=lambda x: self.transactions[x].get_timestamp())
            print(f"T{youngest_tid} aborts due to deadlock")
            self.abort(youngest_tid)

    def _detect_cycle(self, tid: int, visited: set, path: list) -> list:
        if tid in path:
            return path[path.index(tid):]
        if tid in visited:
            return []
        visited.add(tid)
        path.append(tid)
        for neighbor in self.waits_for_graph[tid]:
            result = self._detect_cycle(neighbor, visited, path)
            if result:
                return result
        path.pop()
        return []


# Displaying to verify correctness before implementing the main driver.
TransactionManager
