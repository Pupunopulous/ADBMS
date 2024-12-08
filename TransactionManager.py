"""
-------------------------------------------------------------------------------
Author(s): Rahi Krishna (rk4748), Tanmay G. Dadhania (tgd8275)
Date: December 8, 2024

Description:
This file contains the TransactionManager class, the central component of the system.
It orchestrates transaction execution across multiple sites, handles concurrency control, and ensures correctness via
mechanisms like failure handling, serialization graph construction, and cycle detection.
It interacts with the DataManager and Transaction classes to implement distributed transaction management.
-------------------------------------------------------------------------------
"""

from Transaction import Transaction
from DataManager import DataManager


class TransactionManager:
    def __init__(self):
        self.sites = {i: DataManager(i) for i in range(1, 11)}      # 10 sites, indexed 1 to 10
        self.transactions = {}                                      # Active transactions: transaction_id -> Transaction object
        self.site_status = {i: "up" for i in range(1, 11)}          # Site status: "up"/"down"
        self.failure_history = {i: [] for i in range(1, 11)}        # Failure history: site_id -> list of failure/recovery events
        self.waiting_read_queue = []                                # Queue for waiting reads: list of (transaction_id, variable, site_id)
        self.serialization_graph = {}                               # Store the transaction serialization graph

        # Initialize data variables
        self.initialize_data()

    def initialize_data(self):
        """
        Initialize the 20 variables across the 10 sites based on their index.
        """
        for i in range(1, 21):  # Variables x1 to x20
            initial_value = 10 * i
            if i % 2 == 0:  # Even-indexed variables
                for site_id in self.sites:
                    self.sites[site_id].write(f"x{i}", initial_value, 0)
            else:  # Odd-indexed variables
                site_id = 1 + (i % 10)
                self.sites[site_id].write(f"x{i}", initial_value, 0)

    def print_serialization_graph(self):
        # Check if there's at least one edge in the graph
        if not any(self.serialization_graph.get(tid) for tid in self.serialization_graph):
            # No edges found, so do not print anything
            return
        print("\n--- Serialization Graph ---")
        for from_tid, edges in self.serialization_graph.items():
            for to_tid, edge_types in edges.items():
                edge_types_str = ",".join(edge_types)
                print(f"T{from_tid} -[{edge_types_str}]-> T{to_tid}")
        print("----------------------------")

    def start_transaction(self, transaction_id, timestamp, is_read_only=False):
        """
        Begin a new transaction.
        """
        print(f"Starting {'read-only ' if is_read_only else ''}transaction T{transaction_id} at timestamp {timestamp}.")
        self.transactions[transaction_id] = Transaction(transaction_id, timestamp, is_read_only)

    def read_intention(self, transaction_id, variable):
        """
        Add a read intention to the transaction's instruction queue and attempt the read immediately.
        Calls the DataManager's read method to retrieve the value.
        Aborts the transaction if no valid site can provide the value.
        """
        if transaction_id not in self.transactions:
            raise Exception(f"Transaction T{transaction_id} does not exist.")

        transaction = self.transactions[transaction_id]

        # Add the variable to the transaction's read set
        transaction.add_read(variable)

        # Determine the sites where the variable is stored
        sites_to_read = []
        variable_index = int(variable[1:])  # Extract the numeric part of the variable name

        if variable_index % 2 == 0:
            # Even-indexed variables (replicated): available on all sites
            sites_to_read = [site_id for site_id, site in self.sites.items()]
        else:
            # Odd-indexed variables (non-replicated): stored on a single site
            site_id = 1 + (variable_index % 10)
            sites_to_read = [site_id]

        # Attempt to read from the available sites
        for site_id in sites_to_read:
            if self.site_status[site_id] == "up":
                try:
                    last_commit_time = self.sites[site_id].get_last_commit(variable, transaction.start_time)

                    if last_commit_time is not None:
                        for failure_time, status in self.failure_history[site_id]:
                            if last_commit_time < failure_time < transaction.start_time:
                                raise Exception("Site not functional during required period.")

                    value = self.sites[site_id].read(variable, transaction.start_time)
                    print(f"Transaction T{transaction_id} read {variable}:{value} from Site {site_id}.")
                    return value  # Return the first successful read
                except Exception as e:
                    pass
            else:
                try:
                    last_commit_time = self.sites[site_id].get_last_commit(variable, transaction.start_time)
                    if last_commit_time is not None:
                        for failure_time, status in self.failure_history[site_id]:
                            if last_commit_time < failure_time < transaction.start_time:
                                raise Exception("Site not functional during required period.")

                    self.waiting_read_queue.append([transaction_id, variable_index, site_id])
                    return
                except Exception as e:
                    pass

        # If no valid site can provide the value, abort the transaction
        print(f"Transaction T{transaction_id} aborted: No valid site could provide the value for {variable}.")
        transaction.status = "aborted"
        return None

    def write_intention(self, transaction_id, variable, value, timestamp):
        """
        Add a write intention to the transaction's instruction queue.
        The actual write will be handled during commit.
        """
        if transaction_id not in self.transactions:
            raise Exception(f"Transaction T{transaction_id} does not exist.")

        transaction = self.transactions[transaction_id]
        transaction.add_write(variable, value, timestamp)

    def commit(self, transaction_id, time):
        """
        Commit a transaction after validation.
        Implements:
        1. First Committer Wins rule.
        2. Abort if any write timestamp precedes the failure timestamp of a site.
        """
        if transaction_id not in self.transactions:
            raise Exception(f"Transaction T{transaction_id} does not exist.")

        transaction = self.transactions[transaction_id]

        # Check for First Committer Wins violation and failure timestamp validation
        for variable, (value, write_timestamp) in transaction.write_set.items():
            for site_id, site in self.sites.items():
                if self.site_status[site_id] == "up" and (variable in site.variables or variable.startswith("x")):
                    # First Committer Wins Check
                    if variable in site.version_history:
                        last_commit_time = site.version_history[variable][-1][1]
                        if last_commit_time > transaction.start_time:
                            print(
                                f"Transaction T{transaction_id} aborted: {variable} was committed at {last_commit_time}, "
                                f"after transaction start time {transaction.start_time}.")
                            transaction.status = "aborted"
                            return

                # Failure Timestamp Validation
                for failure_timestamp, status in self.failure_history[site_id]:
                    if status == "down" and write_timestamp < failure_timestamp:
                        print(
                            f"Transaction T{transaction_id} aborted: Write timestamp {write_timestamp} for {variable} "
                            f"precedes failure timestamp {failure_timestamp} on Site {site_id}.")
                        transaction.status = "aborted"
                        return

        for variable, (value, write_timestamp) in transaction.write_set.items():
            # Distribute writes to the appropriate sites
            written_sites = set()
            variable_index = int(variable[1:])  # Extract the numeric part of the variable name

            if variable_index % 2 == 0:
                # Even-indexed variables: Write to all sites that are up
                for site_id, site in self.sites.items():
                    if self.site_status[site_id] == "up" and variable in site.variables:
                        # Retrieve the last recovery timestamp
                        last_recovery_time = max(
                            (timestamp for timestamp, status in self.failure_history[site_id] if status == "up"),
                            default = None
                        )

                        # Check if the write timestamp is valid
                        if last_recovery_time is not None and write_timestamp < last_recovery_time:
                            continue

                        # Perform the write and track the site
                        site.write(variable, value, write_timestamp)
                        written_sites.add(site_id)
            else:
                # Odd-indexed variables: Write to a single designated site
                site_id = 1 + (variable_index % 10)
                if self.site_status[site_id] == "up" and variable in self.sites[site_id].variables:
                    # Retrieve the last recovery timestamp
                    last_recovery_time = max(
                        (timestamp for timestamp, status in self.failure_history[site_id] if status == "up"),
                        default = None
                    )

                    # Check if the write timestamp is valid
                    if last_recovery_time is not None and write_timestamp < last_recovery_time:
                        continue

                    # Perform the write and track the site
                    self.sites[site_id].write(variable, value, write_timestamp)
                    written_sites.add(site_id)

            # Print the sites written to in a single line
            if written_sites:
                written_sites_list = sorted(written_sites)  # Sort for consistent output
                print(f"Transaction T{transaction_id} wrote {variable} to sites: {', '.join(map(str, written_sites_list))}")

        transaction.commit_time = time
        # print(f"T{transaction.transaction_id} commit time = {transaction.commit_time}\n" )

        # Construct edges in the serialization graph
        for other_tid, other_txn in self.transactions.items():
            if other_tid == transaction_id or other_txn.status != "committed":
                continue

            # RW Edge: T(transaction_id) read something that other_tid wrote
            if any(var in other_txn.write_set for var in self.transactions[transaction_id].read_set):
                self.add_dependency(transaction, other_txn, "rw")

            # WR Edge: T(transaction_id) wrote something that other_tid read
            if any(var in self.transactions[transaction_id].write_set for var in other_txn.read_set):
                self.add_dependency(other_txn, transaction, "wr")

            # WW Edge: both wrote to the same variable
            if any(var in self.transactions[transaction_id].write_set for var in other_txn.write_set):
                self.add_dependency(other_txn, transaction, "ww")

        # Print serialization graph for debugging
        self.print_serialization_graph()

        # Check for cycles in the serialization graph
        cycle = self.has_cycle()
        if cycle and self.has_consecutive_rw_edges(cycle):
            last_tid = self.get_last_transaction_in_cycle(cycle)
            print(f"Cycle with consecutive RW edges detected! Transaction T{last_tid} aborted.")
            self.transactions[last_tid].status = "aborted"
            self.remove_transaction_from_graph(last_tid)
            return

        # Mark the transaction as committed
        transaction.status = "committed"
        print(f"Transaction T{transaction_id} has been committed.")

    def update_site_status(self, site_id, status, timestamp):
        """
        Update the status of a site (up/down).
        Records the failure or recovery event with a timestamp.
        """
        if site_id not in self.sites:
            raise Exception(f"Site {site_id} does not exist.")

        if status == "down":
            if self.site_status[site_id] != "down":
                self.sites[site_id].fail()
                self.failure_history[site_id].append((timestamp, "down"))
                self.site_status[site_id] = status
        elif status == "up":
            if self.site_status[site_id] != "up":
                # Recover the site
                self.sites[site_id].recover()
                self.failure_history[site_id].append((timestamp, "up"))
                self.site_status[site_id] = status
                print(f"Site {site_id} has been recovered.")

                # Process the waiting read queue
                for entry in list(self.waiting_read_queue):  # Use a copy to allow modification during iteration
                    transaction_id, variable_index, waiting_site_id = entry
                    if waiting_site_id == site_id:  # Check if the recovered site matches the waiting read site
                        variable = f"x{variable_index}"  # Convert the variable index back to its name
                        try:
                            value = self.sites[site_id].read(variable, self.transactions[transaction_id].start_time)
                            print(f"Transaction T{transaction_id} read {variable}:{value} from recovered Site {site_id}.")
                            # Remove the entry from the queue since it has been processed
                            self.waiting_read_queue.remove(entry)
                        except Exception as e:
                            print(f"Transaction T{transaction_id} failed to read {variable} from recovered Site {site_id}: {e}")

        self.site_status[site_id] = status

    def get_failure_history(self, site_id):
        """
        Retrieve the failure history of a specific site.
        """
        if site_id not in self.failure_history:
            raise Exception(f"Site {site_id} does not exist.")
        return self.failure_history[site_id]

    def querystate(self):
        """
        Print the current state of the system for debugging.
        """
        print("\n--- Dump State ---")
        for site_id, dm in self.sites.items():
            # Sort variables by the numeric part of their names
            sorted_variables = sorted(dm.variables.items(), key=lambda item: int(item[0][1:]))

            # Format variable-value pairs as a string
            site_data = ", ".join(f"{var}: {val}" for var, val in sorted_variables)

            # Check the site status and include it in the output
            if self.site_status[site_id] == "down":
                print(f"site {site_id} (down) – {site_data}")
            else:
                print(f"site {site_id} – {site_data}")
        print("--------------------")

    def add_dependency(self, from_txn, to_txn, edge_type):
        """
        Add a directed edge with a given edge type to the serialization graph.
        """
        if from_txn.transaction_id not in self.serialization_graph:
            self.serialization_graph[from_txn.transaction_id] = {}
        if to_txn.transaction_id not in self.serialization_graph[from_txn.transaction_id]:
            self.serialization_graph[from_txn.transaction_id][to_txn.transaction_id] = set()

        if from_txn.commit_time > to_txn.commit_time:
            self.serialization_graph[from_txn.transaction_id][to_txn.transaction_id].add(edge_type)
        else:
            self.serialization_graph[from_txn.transaction_id][to_txn.transaction_id].add(edge_type[::-1])

    def remove_transaction_from_graph(self, tid):
        """
        Remove a transaction from the graph.
        """
        if tid in self.serialization_graph:
            del self.serialization_graph[tid]
        for from_tid in list(self.serialization_graph.keys()):
            if tid in self.serialization_graph[from_tid]:
                del self.serialization_graph[from_tid][tid]

    def has_cycle(self):
        """
        Detect a cycle in the serialization graph using DFS and return the cycle if found.
        """
        visited = set()
        stack = []
        parent_map = {}  # To track the path and reconstruct the cycle

        def dfs(node):
            if node in stack:  # Cycle detected
                cycle_start_index = stack.index(node)
                return stack[cycle_start_index:]  # Return the cycle as a list of nodes
            if node in visited:
                return None

            visited.add(node)
            stack.append(node)
            for neighbor in self.serialization_graph.get(node, {}):
                parent_map[neighbor] = node
                result = dfs(neighbor)
                if result:
                    return result
            stack.pop()
            return None

        for node in self.serialization_graph:
            stack.clear()
            result = dfs(node)
            if result:  # A cycle is detected
                return result

        return None

    def has_consecutive_rw_edges(self, cycle):
        """
        Check if the given cycle contains two consecutive RW edges.
        """
        if not cycle:
            return False

        for i in range(len(cycle)):
            from_tid = cycle[i]
            to_tid = cycle[(i + 1) % len(cycle)]  # Next transaction in the cycle
            edge_types = self.serialization_graph[from_tid][to_tid]
            if "rw" in edge_types:
                # Check the next edge in the cycle
                next_from_tid = to_tid
                next_to_tid = cycle[(i + 2) % len(cycle)]
                next_edge_types = self.serialization_graph[next_from_tid][next_to_tid]
                if "rw" in next_edge_types:
                    return True

        return False

    def get_last_transaction_in_cycle(self, cycle):
        """
        Identify the last transaction in the cycle based on the latest commit time.
        """
        return max(cycle, key = lambda tid: self.transactions[tid].commit_time)

