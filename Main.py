"""
-------------------------------------------------------------------------------
Author(s): Rahi Krishna (rk4748), Tanmay G. Dadhania (tgd8275)
Date: December 8, 2024

Description:
This file serves as the main entry point for the replicated concurrency system.
It reads and processes commands from an input file, such as starting transactions, reading/writing variables,
managing site failures/recoveries, and committing transactions.
It acts as a bridge between user input and the core functionality provided by the TransactionManager.
-------------------------------------------------------------------------------
"""

import re
import sys
import argparse
from TransactionManager import TransactionManager


class Main:
    @staticmethod
    def main(input_file: str) -> None:
        transaction_manager = TransactionManager()

        try:
            with open(input_file, "r") as file:
                timestamp = 1  # Logical timestamp to simulate the order of operations

                for line in file:
                    line = line.strip()

                    # Ignore empty lines and lines starting with comments ('//' or '#')
                    if not line or line.startswith("//") or line.startswith("#"):
                        continue

                    # Parse command using regex to handle formats like `command(arg1, arg2, ...)`
                    match = re.match(r"(\w+)\s*\(\s*([^)]*)\s*\)", line)
                    if not match:
                        print(f"Invalid command format: {line}", file=sys.stderr)
                        continue

                    command, args = match.groups()
                    args = [arg.strip() for arg in args.split(",")] if args else []

                    # Handle commands
                    if command == "begin":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        transaction_manager.start_transaction(transaction_id, timestamp)

                    elif command == "R":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        variable_id = int(args[1][1:])  # Strip 'x' and convert to int
                        transaction_manager.read_intention(transaction_id, f"x{variable_id}")

                    elif command == "W":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        variable_id = int(args[1][1:])  # Strip 'x' and convert to int
                        value = int(args[2])  # Convert value to int
                        transaction_manager.write_intention(transaction_id, f"x{variable_id}", value, timestamp)

                    elif command == "fail":
                        site_id = int(args[0])  # Convert to int
                        transaction_manager.update_site_status(site_id, "down", timestamp)

                    elif command == "recover":
                        site_id = int(args[0])  # Convert to int
                        transaction_manager.update_site_status(site_id, "up", timestamp)

                    elif command == "end":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        transaction_manager.commit(transaction_id, timestamp)

                    elif command == "dump":
                        transaction_manager.querystate()

                    else:
                        print(f"Unknown command: {command}", file = sys.stderr)

                    # Increment the logical timestamp after processing each line
                    timestamp += 1

        except Exception as e:
            print(f"Error: {e}", file = sys.stderr)


if __name__ == "__main__":
    """
    input_file = "test.txt"
    Main.main(input_file)
    """

    # Use argparse to handle command-line arguments
    parser = argparse.ArgumentParser(description = "Replicated Concurrency Control and Recovery")
    parser.add_argument(
        "input_file",
        type = str,
        help = "Path to the input file containing transaction commands"
    )
    args = parser.parse_args()

    # Pass the input file to the main function
    Main.main(args.input_file)
