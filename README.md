# Replicated Concurrency Control and Recovery System
---

## Authors: 
- **Rahi Krishna: `rk4748`**
- **Tanmay G. Dadhania: `tgd8275`**

---

This project implements a **Replicated Concurrency Control and Recovery System**. It processes a sequence of commands related to transaction management, concurrency control, and recovery in a distributed database system. Please peruse the design document for complete working details.

The system reads transaction commands from input files stored in the `tests/` directory, processes them, and writes the outputs to corresponding files in the `outputs/` directory.

---

## Features
- **Transaction Management**: Handles starting, reading, writing, and committing transactions.
- **Concurrency Control**: Simulates operations on replicated variables across multiple sites.
- **Recovery Management**: Supports site failures and recoveries with timestamped operations.
- **Command Parsing**: Reads commands from `.txt` files in a structured format.
- **Result Logging**: Saves results for each test file in the `outputs/` directory.

---

## How to Run

### 1. **Prerequisites**
- Python 3+ installed on your machine.

### 2. **Directory Setup**
Ensure you have the following directories:
- `tests/`: Place all your input test `.txt` files here.
- `outputs/`: This will be created dynamically if it doesn't exist.

### 3. **Running the Code**
1. (Optional) Clone the repository:
  ```bash
  https://github.com/Pupunopulous/ADBMS.git
  cd ADBMS
  ```
2. Run the script:
  ```bash
  python Main.py
  ```
3. View the results in the outputs/ directory:
  ```bash
  ls outputs/
  cat test{X}.txt.out
  ```
---

## Test Input Format

Commands in the input files follow a specific structure:

- **Start a transaction**: `begin(T1)`
- **Read a variable**: `R(T1, x2)`
- **Write a variable**: `W(T1, x2, 30)`
- **Fail a site**: `fail(2)`
- **Recover a site**: `recover(2)`
- **Commit a transaction**: `end(T1)`
- **Dump system state**: `dump()`

Each line represents a single command. Empty lines or comments (starting with `//` or `#`) are ignored.

---

## Using ReproZip to Reproduce Results

To ensure reproducibility of results, you can use **ReproUnzip** to unpack and run the environment provided in a ReproZip bundle. Follow the steps below:

### 1. **Install ReproUnzip**
Install ReproUnzip using `pip`:
  ```bash
  pip install reprounzip
  ```

### 2. **Unpack the Bundle**
You can unpack the project as follows:
  ```bash
  reprounzip directory setup RepCRec.rpz RepCRec
  reprounzip directory run RepCRec
  ```

### 3. **Verify Outputs**
You can find all files within the `root` directory of the unzipped folder as follows:
  ```bash
  cd RepCRec/root/home/rk4748/RepCRec/
  ```
The input test files will be within the `tests/` folder.

The output files for these tests will be within the `outputs/` folder.

### 4. **Change Inputs and test Reprozip executable**
For testing new input sequences on the Reprozip executable, move into the inputs folder within the unzipped directory, and add your new file here.
  ```bash
  cd RepCRec/root/home/rk4748/RepCRec/root/home/rk4748/RepCRec/tests/
  nano newtest.txt
  ```
Finally, run the executable from the home directory again:
  ```bash
  reprounzip directory run RepCRec
  ```
