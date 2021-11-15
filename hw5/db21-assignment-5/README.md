# Assignment 5
In this assignment, you are asked to implement the two-version two-phase locking (2V2PL) as concurrency control for VanillaDB.

## Steps
To complete this assignment, you need to

1. Fork the Assignment 5 project
2. Trace the code in `as5-core`'s `org.vanilladb.core.storage.tx.concurrency`, `org.vanilladb.core.storage.record` and `org.vanilladb.core.storage.tx.Transaction`
3. Implement 2V2PL (its algorithm is described below)
4. Test correctness using provided test cases
5. Run experiments using provided benchmark project
6. Write a report describing your implementation of 2V2PL and analyzing experiments you performed
7. Push your repository to Gitlab and open a merge request

## 2V2PL
2V2PL is one simple variant of the two-phase locking (2PL). The main purpose of 2V2PL is to avoid **writes blocking reads**, thus improving read concurrency. 2V2PL allows exclusive lock to be compatible with shared locks, but introduces a new type of lock, certify locks, which block all types of locks. In 2V2PL, a transaction that attempts to modify a record follows the procedure below:

1. Acquire exclusive lock on the record
2. Modify the record in **private workspace** (any modifications to the record are invisible to other transactions before the transaction commits)

Please note that in the second step, the modifications are invisible to other transactions, but are visible to the transaction making the modifications. That is, every read must be able to see its preceding writes **within a transaction**.

When a transaction commits, it needs to make its modifications visible with the following procedure:

1. Convert all exclusive locks to cetify locks
2. In-place update the modified records, which are visible to other transactions

The compatibility table of the 2V2PL is provided as follows:

### Compatibility table (with multi-granularity locking; C: certify lock)

|     | S | X | C | IS | IX | SIX |
|-----|---|---|---|----|----|-----|
| S   | O | O | X | O  | X  | X   |
| X   | O | X | X | O  | X  | X   |
| C   | X | X | X | X  | X  | X   |
| IS  | O | O | X | O  | O  | O   |
| IX  | X | X | X | O  | O  | X   |
| SIX | X | X | X | O  | X  | X   |

In this assignment, you only need to implement 2V2PL for data records. **Index structures are not in our requirement.**

## Constraints
Here are some constraints you must follow during coding

- You should mainly modify the code in `org.vanilladb.core.storage.tx` and `org.vanilladb.core.storage.record`. However, you are **allowed** to modify other code if necessary (but should be as few as possible). Please explicitly specify the reason of these modifications.
- Your 2V2PL implementation must at least pass the provided test cases.

If you follows the above rules and hand in your code along with your report in time, you will get at least 60% of scores. The rest of the score will be given by the correctness (in logic), performance and your analysis of experimental result.

You can run the `QueryTestSuite` JUnit test to check the correctness of your implementation.

**Please run the test by right clicking QueryTestSuite.java and run as junit test, or you may get errors on deleting files.**

You only need to consider the SERIALIZABLE isolation level. We will not check other isolation levels (i.e., READ COMMITTED and REPEATABLE READ).

We value the **correctness** of your implementation more than its **performance improvement**, that is, you may find your 2V2PL perform worse than the traditional 2PL, but still get a good score. However, you are not likely to get a good score if the implementation has obvious defects even though you achieve a significant performance improvement.

## Experiments

### Parameters

This time, we provide the TPC-C and a micro-benchmark to measure your performance. The micro-benchmark is very simliar to the benchmarker in assignment 2.

There are some parameters in the micro-benchmark you can adjust:

- `NUM_RTES` (value > 1) - The number of clients
- `RW_TX_RATE` (1.0 >= value >= 0.0) - The probability of generating a write transaction
- `TOTAL_READ_COUNT` (value >= 1) - The number of records read by a transaction
- `LOCAL_HOT_COUNT` (TOTAL_READ_COUNT >= value >= 0) - The number of **hot** records read by a transaction
- `WRITE_RATIO_IN_RW_TX` (1.0 >= value >= 0.0) - The ratio of writes to the total reads of a transaction
- `HOT_CONFLICT_RATE` (0.1 >= value >= 0.001) - The probability of a hot record conclicting with each other

For the TPC-C benchmark, we suggest you to only ajdust this parameter:

- `NUM_WAREHOUSES` - The number of warehouses used in the experiment. This also controls the conflict rate since transactions have less chance to conflict as there are more warehouses.

There is also a configuration in VanillaCore you can try:

- `BUFFER_POOL_SIZE` (value > 1) - The size of buffer pool

Note that it is hard to see the effect of the optimization for `org.vanilladb.core.storage.file` when VanillaCore has a large buffer pool because it makes VanillaCore seldom fetch data from disks.

## The Report

- Briefly explain what you exactly do to implement 2V2PL
- Experiments
  - Your experiement enviornment including (a list of your hardware components, the operating system)
    - e.g. Intel Core i5-3470 CPU @ 3.2GHz, 16 GB RAM, 128 GB SSD, CentOS 7
  - The experiments showing the performance difference between using traditional 2PL and 2V2PL under different parameters (at least adjusting 3 types of parameters)
  - Give a reasonable analysis of your experimental result (both the expected and the unexpected can be mentioned)

Note: There is no strict limitation to the length of your report. Generally, a 2~3 pages report with some figures and tables is fine. **Remember to include all the group members' student IDs in your report.**

## Submission

The procedure of submission is as following:

1. Fork our [Assignment 5](https://shwu10.cs.nthu.edu.tw/courses/databases/2021-spring/db21-assignment-5) on GitLab
2. Clone the repository you forked
3. Finish your work and write the report
4. Commit your work, push your work to GitLab.
  - Name your report as `[Team Member 1 ID]_[Team Member 2 ID]_assignment5_report`
    - E.g. `102062563_103062528_assignment5_report.pdf`
5. Open a merge request to the original repository.
  - Source branch: Your working branch.
  - Target branch: The branch with your team number. (e.g. `team-1`)
  - Title: `Team-X Submission` (e.g. `Team-1 Submission`).

**Important: We do not accept late submission.**

## No Plagiarism Will Be Tolerated

If we find you copy someone’s code, you will get 0 point for this assignment.

## Demo

Due to the complexity of this assignment, we hope you can come to explain your work face to face. We will announce a demo time registration table after submission. Don't forget to choose the demo time for your team.

## Hint

- Take a look at the class `LockTable`, and observe how we implement the compatibility table in the absence of certify lock
- You may want to modify the class `Transaction` to create a per-transaction private workspace
- Every update to a record will eventually reach the `setVal()` method of `RecordFile`, which might be a good timing to perform either a write on private workspace or an in-place write
- If you write a new concurrency control manager, you should set the `*_CONCUR_MGR` properties in `vanilladb.properties` to your own classes
- Reference: Concurrency Control and Recovery in Database Systems Chapter 5.4 (link: https://drive.google.com/open?id=1VUpHlmZpsUqJ-8VhqjlVDNn52Wi34Gk0)

## Deadline
Sumbit your work before **2021/5/26 (Wed.) 23:59:59**.
