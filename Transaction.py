"""
-------------------------------------------------------------------------------
Author(s): Rahi Krishna (rk4748), Tanmay G. Dadhania (tgd8275)
Date: December 8, 2024

Description:
This file defines the Transaction class, encapsulating the state and behavior of a single transaction.
It tracks transaction metadata (e.g., ID, start time, status), read and write sets, and whether the transaction is
read-only.
It provides methods to add read/write intentions and manage the transaction's lifecycle.
-------------------------------------------------------------------------------
"""

class Transaction:
    def __init__(self, transaction_id, start_time, is_read_only=False):
        self.transaction_id = transaction_id
        self.start_time = start_time        # Logical start time of the transaction
        self.commit_time = None             # Commit time of the current transaction
        self.is_read_only = is_read_only    # Whether the transaction is read-only
        self.read_set = set()               # Variables read by this transaction
        self.write_set = {}                 # Variables written by this transaction: variable -> value
        self.status = "active"              # Status of the transaction: "active", "committed", or "aborted"

    def add_read(self, variable):
        """
        Add a variable to the transaction's read set.
        Ensures no duplicate entries.
        """
        if variable not in self.read_set:
            self.read_set.add(variable)

    def add_write(self, variable, value, timestamp):
        """
        Add a variable and its value to the transaction's write set.
        Overwrites any previous value for the same variable in the write set.
        """
        self.write_set[variable] = [value,timestamp]
