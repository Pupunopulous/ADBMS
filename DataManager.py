"""
-------------------------------------------------------------------------------
Author(s): Rahi Krishna (rk4748), Tanmay G. Dadhania (tgd8275)
Date: December 8, 2024

Description:
This file implements the DataManager class, responsible for managing data storage and retrieval at individual sites.
It handles operations such as reading and writing variables, maintaining version histories, and supporting failure
recovery mechanisms for distributed data replication.
-------------------------------------------------------------------------------
"""

class DataManager:
    def __init__(self, site_id):
        self.site_id = site_id
        self.variables = {}  # Tracks current committed values: variable -> value
        self.status = "up"  # Current status of the site: "up" or "down"
        self.version_history = {}  # Tracks version history: variable -> list of [value, commit_time]
        self.committed_after_recovery = set()  # Tracks variables committed to after recovery

    def read(self, variable, start_time):
        """
        Return the value of a variable for a transaction's snapshot.
        A transaction sees the most recent version committed before its start time.
        Reads are permitted only if the site is up and a write has been committed
        to the variable after recovery (if applicable).
        """
        if self.status != "up":
            raise Exception(f"Site {self.site_id} is down, cannot read {variable}.")
        if variable not in self.version_history:
            raise Exception(f"Variable {variable} not found at Site {self.site_id}.")
        # if variable not in self.committed_after_recovery:
        #    raise Exception(f"Variable {variable} has not been committed to after recovery at Site {self.site_id}.")

        # Find the most recent version committed before start_time
        for value, commit_time in reversed(self.version_history[variable]):
            if commit_time <= start_time:
                return value

        raise Exception(f"No valid version of {variable} found at Site {self.site_id} for start_time {start_time}.")

    def get_last_commit(self, variable, start_time):
        """
        Return the value of a variable for a transaction's snapshot.
        A transaction sees the most recent version committed before its start time.
        Reads are permitted only if the site is up and a write has been committed
        to the variable after recovery (if applicable).
        """
        if self.status != "up":
            raise Exception(f"Site {self.site_id} is down, cannot read {variable}.")
        if variable not in self.version_history:
            raise Exception(f"Variable {variable} not found at Site {self.site_id}.")
        # if variable not in self.committed_after_recovery:
        #    raise Exception(f"Variable {variable} has not been committed to after recovery at Site {self.site_id}.")

        # Find the most recent version committed before start_time
        for value, commit_time in reversed(self.version_history[variable]):
            if commit_time <= start_time:
                return commit_time

        raise Exception(f"No valid version of {variable} found at Site {self.site_id} for start_time {start_time}.")

    def write(self, variable, value, commit_time):
        """
        Write a new value to a variable.
        The new version is added to the version history, and the variable
        is marked as committed after recovery.
        """
        if self.status != "up":
            raise Exception(f"Site {self.site_id} is down, cannot write to {variable}.")

        if variable not in self.version_history:
            self.version_history[variable] = []

        # Append the new version to the history
        self.version_history[variable].append([value, commit_time])
        self.variables[variable] = value  # Update the current value
        self.committed_after_recovery.add(variable)  # Mark as committed after recovery
        # print(f"Wrote {variable} = {value} at Site {self.site_id} with commit_time {commit_time}.")

    def fail(self):
        """
        Simulate a site failure.
        All operations (read/write) will fail until recovery.
        Clears the variable histories except for the last committed one.
        """
        self.status = "down"
        self.committed_after_recovery.clear()  # Clear tracking for replicated variables

        # Retain only the last committed entry in the version history
        for variable, history in self.version_history.items():
            if history:
                # Keep only the last committed entry
                self.version_history[variable] = [history[-1]]  # Clear the committed-after-recovery tracker
        # print(f"Site {self.site_id} has failed.")

    def recover(self):
        """
        Simulate a site recovery.
        - Non-replicated variables (odd-numbered) are tracked for availability.
        - Replicated variables (even-numbered) are immediately available for writes but not reads
        until consistency is re-established.
        """
        self.status = "up"
        # print(f"Site {self.site_id} has recovered.")

        # Update the availability of variables
        for variable in self.variables:
            variable_index = int(variable[1:])  # Extract the numeric part of the variable name

            if variable_index % 2 != 0:
                # Non-replicated (odd-numbered) variables: require tracking for reads after recovery
                if variable not in self.committed_after_recovery:
                    self.committed_after_recovery.add(variable)  # Add to post-recovery tracking
