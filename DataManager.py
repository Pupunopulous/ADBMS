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

        # Find the most recent version committed before start_time
        for value, commit_time in reversed(self.version_history[variable]):
            if commit_time <= start_time:
                return value

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

    def get_last_committed_version(self, variable, start_time):
        """
        Return (value, commit_time) of the most recent version of `variable` committed
        on this site before `start_time`. If none found, return None.
        """
        if variable not in self.version_history:
            return None

        for value, commit_time in reversed(self.version_history[variable]):
            if commit_time <= start_time:
                return (value, commit_time)
        return None

    def is_commit_time_valid(self, commit_time, transaction_start_time, failure_history):
        """
        Check if commit_time is valid given the site's failure/recovery history
        and the transaction's start_time.
        It returns True if no failures happened between commit_time and transaction_start_time.
        """
        # If failure_history is empty, no failures to worry about
        if not failure_history:
            return True

        for event_time, status in failure_history:
            if commit_time < event_time < transaction_start_time and status == "down":
                # There was a site failure after this version was committed but before the transaction started
                return False
        return True

    def can_write_variable(self, variable, write_timestamp, transaction_start_time, failure_history):
        """
        Checks if writing `variable` at `write_timestamp` is valid.
        - First Committer Wins: If any commit_time in the version history is greater than transaction_start_time,
          it means someone committed later, and this transaction should abort.
        - Site Failure Check: If site was down after `write_timestamp` but before transaction_start_time,
          then the write is invalid.
        """

        # Check First Committer Wins
        if variable in self.version_history:
            for val, commit_time in self.version_history[variable]:
                if commit_time > transaction_start_time:
                    # Another transaction committed after we started. Abort needed.
                    return False

        # Check site failures invalidating the write
        for event_time, status in failure_history:
            if status == "down" and write_timestamp < event_time < transaction_start_time:
                # The site failed after we intended to write, invalidating this write
                return False

        return True

    def get_readable_value_after_recovery(self, variable, transaction_start_time, failure_history):
        """
        After a site recovers, we want to ensure the transaction can read `variable`.
        This method finds the appropriate version and checks if it's valid given the history.
        Returns the value if found and valid, otherwise None.
        """
        version = self.get_last_committed_version(variable, transaction_start_time)
        if version is None:
            return None

        value, commit_time = version
        if self.is_commit_time_valid(commit_time, transaction_start_time, failure_history):
            return value
        return None

