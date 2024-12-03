import re
import sys

from TransactionManager import TransactionManager


class Main:
    @staticmethod
    def main(input_file: str) -> None:
        transaction_manager = TransactionManager()
        try:
            with open(input_file, "r") as file:
                timestamp = 1
                for line in file:
                    line = line.strip()
                    if not line:
                        continue

                    # Parse command using regex to handle formats like `command(arg1,arg2,...)`
                    match = re.match(r"(\w+)\(([^)]*)\)", line)
                    if not match:
                        continue

                    command, args = match.groups()
                    args = args.split(",") if args else []

                    if command == "begin":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        transaction_manager.begin(transaction_id, timestamp)
                    elif command == "beginRO":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        transaction_manager.begin_ro(transaction_id, timestamp)
                    elif command == "R":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        variable_id = int(args[1][1:])  # Strip 'x' and convert to int
                        transaction_manager.read(transaction_id, variable_id, timestamp)
                    elif command == "W":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        variable_id = int(args[1][1:])  # Strip 'x' and convert to int
                        value = int(args[2])  # Convert value to int
                        transaction_manager.write(transaction_id, variable_id, value, timestamp)
                    elif command == "fail":
                        site_id = int(args[0])  # Convert to int
                        transaction_manager.fail(site_id)
                    elif command == "recover":
                        site_id = int(args[0])  # Convert to int
                        transaction_manager.recover(site_id)
                    elif command == "end":
                        transaction_id = int(args[0][1:])  # Strip 'T' and convert to int
                        transaction_manager.end(transaction_id, timestamp)
                    elif command == "dump":
                        transaction_manager.dump()
                    else:
                        print(f"Unknown command: {command}", file=sys.stderr)

                    timestamp += 1

        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)

if __name__ == "__main__":
    # Provide the path to your test case file
    input_file = "test1.txt"
    Main.main(input_file)
