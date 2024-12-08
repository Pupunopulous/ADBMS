from Transaction import Transaction
from DataManager import DataManager


class TransactionManager:
    def __init__(self):
        self.sites = {i: DataManager(i) for i in range(1, 11)}  # 10 sites, indexed 1 to 10
        self.transactions = {}  # Active transactions: transaction_id -> Transaction object
        self.site_status = {i: "up" for i in range(1, 11)}  # Site status: "up"/"down"
        self.failure_history = {i: [] for i in range(1, 11)}  # Failure history: site_id -> list of failure/recovery events
        self.waiting_read_queue = []  # Queue for waiting reads: list of (transaction_id, variable, site_id)
        self.serialization_graph = {}  # Graph to represent dependencies: {TID -> [TIDs it depends on]}

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
        if not any(self.serialization_graph.values()):
            # No edges found, so do not print anything
            return

        print("\n--- Serialization Graph ---")
        for from_tid, to_tids in self.serialization_graph.items():
            if to_tids:  # Only print if this node has outgoing edges
                print(f"T{from_tid} -> {', '.join(f'T{tid}' for tid in to_tids)}")
        print("---------------------------")

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
                    # Calculate the last commit time for the variable
                    last_commit_time = None
                    for value, commit_time in reversed(self.sites[site_id].version_history[variable]):
                        if commit_time <= transaction.start_time:
                            last_commit_time = commit_time
                            break

                    # Check failure history before attempting the read
                    if last_commit_time is not None:
                        for failure_time, status in self.failure_history[site_id]:
                            if last_commit_time < failure_time < transaction.start_time:
                                # print(f"Skipping Site {site_id} for {variable}: Last commit time {last_commit_time} is invalid "
                                #    f"due to failure at {failure_time}.")
                                raise Exception("Site not functional during required period.")

                    # Attempt to read from the site
                    value = self.sites[site_id].read(variable, transaction.start_time)
                    print(f"Transaction T{transaction_id} read {variable}:{value} from Site {site_id}.")
                    return value  # Return the first successful read
                except Exception as e:
                    pass
                    # print(f"Transaction T{transaction_id} failed to read {variable} from Site {site_id}: {e}")
            else:
                try:
                    # Calculate the last commit time for the variable
                    last_commit_time = None
                    for value, commit_time in reversed(self.sites[site_id].version_history[variable]):
                        if commit_time <= transaction.start_time:
                            last_commit_time = commit_time
                            break

                    # Check failure history before attempting the read
                    if last_commit_time is not None:
                        for failure_time, status in self.failure_history[site_id]:
                            if last_commit_time < failure_time < transaction.start_time:
                                # print(f"Skipping Site {site_id} for {variable}: Last commit time {last_commit_time} is invalid "
                                #    f"due to failure at {failure_time}.")
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
        # print(f"Transaction T{transaction_id} added write intention for {variable} = {value}.")

    def commit(self, transaction_id, time):
        """
        Commit a transaction after validation using DataManager methods.
        This method now delegates:
        - First Committer Wins checks
        - Failure timestamp checks
        - Variable availability checks
        to the DataManager via `can_write_variable`.

        Steps:
        1. Validate that each intended write is permissible using DataManager's `can_write_variable`.
        2. If validation passes for all writes, apply the writes.
        3. Update the serialization graph.
        4. Check for cycles; if found, abort the youngest transaction in the cycle.
        5. Otherwise, mark this transaction as committed.
        """
        if transaction_id not in self.transactions:
            raise Exception(f"Transaction T{transaction_id} does not exist.")

        transaction = self.transactions[transaction_id]

        # Validate each write before committing
        for variable, (value, write_timestamp) in transaction.write_set.items():
            variable_index = int(variable[1:])
            if variable_index % 2 == 0:
                # Even-indexed: replicated at all sites
                for site_id, site in self.sites.items():
                    if self.site_status[site_id] == "up" and variable in site.variables:
                        # Check if we can write this variable to this site
                        if not site.can_write_variable(variable, write_timestamp, transaction.start_time,
                                                       self.failure_history[site_id]):
                            print(f"Transaction T{transaction_id} aborted: Cannot write {variable} to Site {site_id}.")
                            transaction.status = "aborted"
                            return
            else:
                # Odd-indexed: stored at a single site
                site_id = 1 + (variable_index % 10)
                if self.site_status[site_id] == "up" and variable in self.sites[site_id].variables:
                    if not self.sites[site_id].can_write_variable(variable, write_timestamp, transaction.start_time,
                                                                  self.failure_history[site_id]):
                        print(f"Transaction T{transaction_id} aborted: Cannot write {variable} to Site {site_id}.")
                        transaction.status = "aborted"
                        return
                else:
                    # If the designated site is down or doesn't have the variable, we must abort
                    print(
                        f"Transaction T{transaction_id} aborted: Site {site_id} is not available for writing {variable}.")
                    transaction.status = "aborted"
                    return

        # If all validations pass, perform the actual writes
        for variable, (value, write_timestamp) in transaction.write_set.items():
            written_sites = set()
            variable_index = int(variable[1:])
            if variable_index % 2 == 0:
                # Even-indexed variables: write to all up sites that have the variable
                for site_id, site in self.sites.items():
                    if self.site_status[site_id] == "up" and variable in site.variables:
                        site.write(variable, value, write_timestamp)
                        written_sites.add(site_id)
            else:
                # Odd-indexed variables: write to the single designated site
                site_id = 1 + (variable_index % 10)
                if self.site_status[site_id] == "up" and variable in self.sites[site_id].variables:
                    self.sites[site_id].write(variable, value, write_timestamp)
                    written_sites.add(site_id)

            if written_sites:
                written_sites_list = sorted(written_sites)
                print(
                    f"Transaction T{transaction_id} wrote {variable} to sites: {', '.join(map(str, written_sites_list))}")

        # Build edges in the serialization graph
        for other_tid, other_txn in self.transactions.items():
            if other_tid == transaction_id or other_txn.status != "committed":
                continue

            # RW Edge: other_tid wrote a variable that this transaction read
            if any(var in other_txn.write_set for var in transaction.read_set):
                self.add_dependency(transaction_id, other_tid)

            # WR Edge: this transaction wrote a variable that other_tid read
            if any(var in transaction.write_set for var in other_txn.read_set):
                self.add_dependency(other_tid, transaction_id)

            # WW Edge: both wrote the same variable
            if any(var in transaction.write_set for var in other_txn.write_set):
                self.add_dependency(other_tid, transaction_id)

        # Print the serialization graph for debugging
        self.print_serialization_graph()

        # Check for cycles in the serialization graph
        if self.has_cycle():
            last_tid = self.get_last_transaction_in_cycle()
            print(f"Cycle detected! Aborting transaction T{last_tid}.")
            self.transactions[last_tid].status = "aborted"
            self.remove_transaction_from_graph(last_tid)
            return

        # If no cycle, commit the transaction
        transaction.status = "committed"
        print(f"Transaction T{transaction_id} has been committed.")

    def update_site_status(self, site_id, status, timestamp):
        """
        Update the status of a site (up/down).
        If the site recovers, we now use the DataManager's get_readable_value_after_recovery method
        to attempt reads for transactions that were waiting on this site's data.
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

                # Process the waiting read queue for this recovered site
                for entry in list(self.waiting_read_queue):  # use a copy to allow safe removal
                    transaction_id, variable_index, waiting_site_id = entry
                    if waiting_site_id == site_id:
                        variable = f"x{variable_index}"
                        txn = self.transactions.get(transaction_id)
                        if txn is not None and txn.status == "active":
                            # Attempt to read using DataManager's helper
                            value = self.sites[site_id].get_readable_value_after_recovery(
                                variable,
                                txn.start_time,
                                self.failure_history[site_id]
                            )
                            if value is not None:
                                print(
                                    f"Transaction T{transaction_id} read {variable}:{value} from recovered Site {site_id}.")
                                # Remove the entry from the queue since it succeeded
                                self.waiting_read_queue.remove(entry)
                            else :
                                print(f"Transaction T{transaction_id} failed to read {variable} from recovered Site {site_id}.")
            # If the site was already up, no action needed.

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

    def add_dependency(self, from_tid, to_tid):
        """
        Add a directed edge in the serialization graph.
        """
        if from_tid not in self.serialization_graph:
            self.serialization_graph[from_tid] = set()
        self.serialization_graph[from_tid].add(to_tid)

    def remove_transaction_from_graph(self, tid):
        """
        Remove a transaction from the graph.
        """
        if tid in self.serialization_graph:
            del self.serialization_graph[tid]
        for dependencies in self.serialization_graph.values():
            dependencies.discard(tid)

    def has_cycle(self):
        """
        Detect a cycle in the serialization graph.
        """
        visited = set()
        stack = set()

        def dfs(node):
            if node in stack:  # Cycle detected
                return True
            if node in visited:
                return False

            visited.add(node)
            stack.add(node)
            for neighbor in self.serialization_graph.get(node, []):
                if dfs(neighbor):
                    return True
            stack.remove(node)
            return False

        return any(dfs(node) for node in self.serialization_graph)

    def get_last_transaction_in_cycle(self):
        """
        Identify the last transaction in the cycle.
        """
        visited = set()
        stack = []

        def dfs(node):
            if node in stack:
                return node  # Return the node that closes the cycle
            if node in visited:
                return None

            visited.add(node)
            stack.append(node)
            for neighbor in self.serialization_graph.get(node, []):
                result = dfs(neighbor)
                if result is not None:
                    return result
            stack.pop()
            return None

        for node in self.serialization_graph:
            stack.clear()
            result = dfs(node)
            if result is not None:
                return max(stack, key=lambda tid: self.transactions[tid].start_time)

        return None
