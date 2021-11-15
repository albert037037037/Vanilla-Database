# Assignment 2
In this assignment, you are asked to
  - implement a new transaction - **UpdateItemPriceTxn**, which will randomly select some items and update their price, in the benchmark project.
  - modify `org.vanilladb.bench.StatisticMgr` in order to make it produce an additional report with more information.

We will run your code to make sure that all the following requirements meet, so please run it yourself before submission.


## Steps
To complete this assignment, you need to

1. Fork the Assignment 2 project
2. Trace the benchmark project code yourself
3. Implement the **UpdateItemPriceTxn** using JDBC and stored procedures
4. Modify the RTE so that you can control the ratio between **ReadItemTxn** and **UpdateItemPriceTxn**. (You can now compare the performance difference between different read-only / read-write transaction ratio. For example, 40% **ReadItemTxn** and 60% **UpdateItemPriceTxn**)
5. Modify `org.vanilladb.bench.StatisticMgr`.
6. Load the testbed, run a few experiments and write a report


## Loading The Testbed

In this project, we have provided `TestbedLoader`. It will generate `INSERT` commands in order to populate testing data for benchmarking. When you load the testbed, it creates an **item** table and populates 100000 items by default. You can change how many itmes to be populated in `vanillabench.properties`.


## UpdateItemPriceTxn
You should implement a new transaction - **UpdateItemPriceTxn**, which randomly selects some items and raises their price. The detailed information of this transaction is described as following:

- Prepare parameters
  - Randomly pick 10 item ids
  - Randomly generate 10 values (between `0.0` and `5.0`) for the price raise
- Executes SQLs
	- For each item
		- `SELECT` the name and the price of the item
		- Check if the price exceeds `As2BenchConstants.MAX_PRICE`.
			- If the price exceeded, adjust it to `As2BenchConstants.MIN_PRICE`.
			- If it didn't, `UPDATE` the price to **its original price + the raise that it generates in the client**

We have implemented a read-only transaction called **ReadItem** as a reference. Please do not modify the code for that transaction such as `ReadItemTxnJdbcJob`, `ReadItemTxnProc` and `ReadItemProcParamHelper`. You should create new classes for yours.

In addtition, please do not generate parameters in either stored procedres or JDBC jobs. You should create a class named with `...ParamGen` in `org.vanilladb.bench.benchmarks.as2.rte` and generate parameters for `UpdateItemPriceTxn` in it.

**Note**: If you encounter `LockAbortException` during your experiments after you implement **UpdateItemPriceTxn**. Please refer to [this article](https://shwu10.cs.nthu.edu.tw/courses/databases/2021-spring/faq/blob/master/Lock_Abort_Exception_in_Benchmark.md) in FAQ repository.


## Add `READ_WRITE_TX_RATE` Property

We wish to control the ratio of `ReadItemTxn` and `UpdateItemPriceTxn` by simply modifying the `vanillabench.properties`. Please add a property named with `READ_WRITE_TX_RATE` for this purpose.


## StatisticMgr

After running the benchmarker, it produces a report with the following information for now:

```
# of txns (including aborts) during benchmark period: 33678
Details of transactions:
READ_ITEM: 2 ms
READ_ITEM: 3 ms
...
READ_ITEM: 2 ms
READ_ITEM: 2 ms

READ_ITEM 33559 avg latency: 2 ms
Total 33678 Aborted 119 Commited 33559 avg Commited latency: 3 ms
```

As you can see, it only summarizes the throughput (txs/min) and the average latency (ms). However, we usually need more information in order to get a deep insight into the experiment. Hence, you are required to modify `StatisticMgr` to make it produce **another report** with average, minimum, maximum, 25th, median, 75th latency along with throughput in every 5 seconds.

Here is an example of the reports we require:

```
time(sec), throughput(txs), avg_latency(ms), min(ms), max(ms), 25th_lat(ms), median_lat(ms), 75th_lat(ms)
65,1024,40,10,120,23,43,87
70,1051,40,11,130,24,42,85
75,1031,40,12,122,23,46,94
80,1100,39,10,123,22,41,83
...
```

The name of the reports should be `[Year][Month][Day]-[Hour][Minute][Sec]-as2bench.csv`. E.g. `20210222-160720-as2bench.csv`

## The Report
- How you implement the transaction using JDBC and stored procedures briefly. Please do not paste your entire code here.
- A screenshot of any CSV report after you modify the `StatisticMgr`
- Experiements
	- Your experiement enviornment (including a list of your hardware components, the operating system)
		- e.g. Intel Core i5-3470 CPU @ 3.2GHz, 16 GB RAM, 128 GB SSD, CentOS 7
	- The performance (e.g. txs/min) comparison between
		- JDBC and stored procedures
		- different ratio (**at least 3 different ratio**) of **ReadItemTxn** and **UpdateItemPriceTxn**
			- e.g. 100% ReadItemTxn and 0% UpdateItemPriceTxn
			- e.g. 50% ReadItemTxn and 50% UpdateItemPriceTxn
		- (optional) other adjustable parameters
	- **The analysis and explanation for the above experiements**
	- Note: If you are using Windows, you should **turn off** disk write cache feature for correctness. You can find more information [here](https://shwu10.cs.nthu.edu.tw/courses/databases/2021-spring/faq/blob/master/Windows_Disk_Write_Cache.md).
- Anything worth to be mentioned

There is no strict limitation to the length of your report. Generally, a 2~3 pages report with some figures and tables is fine. **Remember to include all the group members' student IDs in your report.**

## Submission

The procedure of submission is as following:

1. Fork our [Assignment 2](https://shwu10.cs.nthu.edu.tw/courses/databases/2021-spring/db21-assignment-2) on GitLab
2. Clone the repository you forked
3. **Set your repository to 'private'**
4. Finish your work and write a report
5. Commit your work, push to GitLab and then open a merge request to submit. The repository should contain
	- *[Project directories]*
	- *[Team Number]*_assignment2_report.pdf (e.g. team1_assignment2_report.pdf)

    Note: Each team only need one submission.


## No Plagiarism Will Be Tolerated

If we find you copy someone’s code, you will get **0 point** for this assignment


## Hints

1. We have already implemented **ReadItem**. You can find the code for JDBC and stored procedures in the following packages:
	- JDBC => `org.vanilladb.bench.benchmarks.as2.rte.jdbc.ReadItemTxnJdbcJob`
	- Stored Procedure => `org.vanilladb.bench.server.procedure.as2.ReadItemTxnProc`
2. You can modify `getNextTxType()` in `org.vanilladb.bench.benchmarks.as2.rte.As2BenchRte` to control which type of transactions will be the next to go.
3. Check out `As2BenchConstants` to see how we get a value from `vanillabench.properties`.
4. When you implement `UpdateItemPriceTxn`, you need method `executeUpdate` which usage is like method `executeQuery` in `ReadItemTxnJdbcJob`.
5. If you encounter any problem, take a look our [FAQ repository](https://shwu10.cs.nthu.edu.tw/courses/databases/2021-spring/faq) first.


## Deadline

Sumbit your work before **2021/04/07 (Wen.) 23:59:59**.

Late submission will NOT be accepted.
