/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.bench.benchmarks.micro.rte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.elasql.bench.benchmarks.micro.ElasqlMicrobenchConstants;
import org.elasql.bench.util.ElasqlBenchProperties;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.micro.MicrobenchTransactionType;
import org.vanilladb.bench.benchmarks.tpcc.TpccValueGenerator;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomNonRepeatGenerator;

public class ElasqlMicrobenchParamGen implements TxParamGenerator<MicrobenchTransactionType> {
	
	// Transaaction Type
	private static final double DIST_TX_RATE;
	private static final double RW_TX_RATE;
	private static final double SKEW_TX_RATE;
	private static final double LONG_READ_TX_RATE;
	
	// Read Counts
	private static final int TOTAL_READ_COUNT;
	private static final int LOCAL_HOT_COUNT; 
	private static final int REMOTE_HOT_COUNT;
	private static final int REMOTE_COLD_COUNT;
	// The local cold count will be calculated on the fly
	
	// Access Pattern
	private static final double WRITE_RATIO_IN_RW_TX;
	private static final double HOT_CONFLICT_RATE;
	private static final double SKEW_PERCENTAGE;
	
	// Data Size
	private static final int DATA_SIZE_PER_PART;
	private static final int HOT_DATA_SIZE_PER_PART;
	private static final int COLD_DATA_SIZE_PER_PART;
	
	// Shortcuts
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	
	// Other parameters
	private static final int RANDOM_SWAP_FACTOR = 20;
	
	// Old variables
	// private static final double LOCALITY;
	// private static final double HOTNESS;
//	private static final int PARTITION_NUM;
	// private static double[] SKEWNESS_DISTRIBUTION;
//	private static final int DATA_SIZE_PER_PART;
//	private static final int USER_COUNT;
//	private static final int DATA_PER_USER;
//	private static final int USER_SESSION_PERIOD = 50;

	static {
		DIST_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".DIST_TX_RATE", 0.0);
		RW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".RW_TX_RATE", 0.0);
		SKEW_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".SKEW_TX_RATE", 0.0);
		LONG_READ_TX_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".LONG_READ_TX_RATE", 0.0);
		
		TOTAL_READ_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".TOTAL_READ_COUNT", 10);
		LOCAL_HOT_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".LOCAL_HOT_COUNT", 1);
		REMOTE_HOT_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".REMOTE_HOT_COUNT", 0);
		REMOTE_COLD_COUNT = ElasqlBenchProperties.getLoader()
				.getPropertyAsInteger(ElasqlMicrobenchParamGen.class.getName() + ".REMOTE_COLD_COUNT", 5);
		
		WRITE_RATIO_IN_RW_TX = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".WRITE_RATIO_IN_RW_TX", 0.5);
		HOT_CONFLICT_RATE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".HOT_CONFLICT_RATE", 0.01);
		SKEW_PERCENTAGE = ElasqlBenchProperties.getLoader()
				.getPropertyAsDouble(ElasqlMicrobenchParamGen.class.getName() + ".SKEW_PERCENTAGE", 0.2);
		
		DATA_SIZE_PER_PART = ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
		HOT_DATA_SIZE_PER_PART = (int) (1.0 / HOT_CONFLICT_RATE);
		COLD_DATA_SIZE_PER_PART = DATA_SIZE_PER_PART - HOT_DATA_SIZE_PER_PART;
		
		// Old code
//		DATA_PER_USER = COLD_DATA_SIZE_PER_PART;
//		USER_COUNT = COLD_DATA_SIZE_PER_PART / DATA_PER_USER;
	}
	
	
	// Old code
//	private static Map<Integer, Integer> itemRandomMap;
//	private SutConnection spc;
//	private Object[] params;
//	private int[] lastWindowOffset = new int[PARTITION_NUM];
//	private int sessionUser = 0;
//	private int sessionCountDown = -1;
//	private long startTime = System.currentTimeMillis();
//	private int wid;
	
	private Random random = new Random();

	public ElasqlMicrobenchParamGen() {
//		// initialize random item mapping map
//		itemRandomMap = new HashMap<Integer, Integer>(TpccConstants.NUM_ITEMS);
//		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(TpccConstants.NUM_ITEMS);
//		for (int i = 1; i <= TpccConstants.NUM_ITEMS; i++)
//			itemRandomMap.put(i, i);
	}

	@Override
	public MicrobenchTransactionType getTxnType() {
		return MicrobenchTransactionType.MICRO_TXN;
	}

	// a main application for debugging
	public static void main(String[] args) {
		ElasqlMicrobenchParamGen executor = new ElasqlMicrobenchParamGen();
		
		System.out.println("Parameters:");
		System.out.println("Distributed Tx Rate: " + DIST_TX_RATE);
		System.out.println("Read Write Tx Rate: " + RW_TX_RATE);
		System.out.println("Skew Tx Rate: " + SKEW_TX_RATE);
		System.out.println("Long Read Tx Rate: " + LONG_READ_TX_RATE);
		System.out.println("Total Read Count: " + TOTAL_READ_COUNT);
		System.out.println("Local Hot Count: " + LOCAL_HOT_COUNT);
		System.out.println("Remote Hot Count: " + REMOTE_HOT_COUNT);
		System.out.println("Remote Cold Count: " + REMOTE_COLD_COUNT);
		System.out.println("Write Ratio in RW Tx: " + WRITE_RATIO_IN_RW_TX);
		System.out.println("Hot Conflict Rate: " + HOT_CONFLICT_RATE);
		System.out.println("Skew Percentage: " + SKEW_PERCENTAGE);
		
		System.out.println("# of items / partition: " + DATA_SIZE_PER_PART);
		System.out.println("# of hot items / partition: " + HOT_DATA_SIZE_PER_PART);
		System.out.println("# of cold items / partition: " + COLD_DATA_SIZE_PER_PART);

		System.out.println();

		for (int i = 0; i < 1000; i++) {
			Object[] params = executor.generateParameter();
			System.out.println(Arrays.toString(params));
		}
	}

	@Override
	public Object[] generateParameter() {
		TpccValueGenerator rvg = new TpccValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();

//		updateSessionUser(rvg);
		// System.out.println("sessionUser: "+sessionUser);
		
		// ================================
		// Decide the types of transactions
		// ================================
		
		boolean isDistributedTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0) ? true : false;
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;
		boolean isSkewTx = (rvg.randomChooseFromDistribution(SKEW_TX_RATE, 1 - SKEW_TX_RATE) == 0) ? true : false;
		boolean isLongReadTx = (rvg.randomChooseFromDistribution(LONG_READ_TX_RATE, 1 - LONG_READ_TX_RATE) == 0) ? true : false;
		
		if (NUM_PARTITIONS < 2)
			isDistributedTx = false;

		
		// =========================================
		// Decide the counts and the main partitions
		// =========================================

		// Choose the main partition
		int mainPartition = 0;
		if (!isSkewTx) {
			// Uniformly select
			mainPartition = rvg.number(0, NUM_PARTITIONS - 1);
		} else {
			// Avoid to choose the first 1/5 partitions
			// because we need to treat them as remote partitions
			int boundaryPartition = (int) (SKEW_PERCENTAGE * NUM_PARTITIONS) - 1;
			boundaryPartition = (boundaryPartition < 0) ? 0 : boundaryPartition;
			mainPartition = rvg.number(boundaryPartition + 1, NUM_PARTITIONS - 1);
		}

		// Set read counts
		int totalReadCount = TOTAL_READ_COUNT;
		int localHotCount = LOCAL_HOT_COUNT;
		int remoteHotCount = 0;
		int remoteColdCount = 0;
		
		// For distributed tx
		if (isDistributedTx) {
			remoteHotCount = REMOTE_HOT_COUNT;
			remoteColdCount = REMOTE_COLD_COUNT;
		}
		
		// For long read tx
		// 10 times of total records and remote colds
		if (isLongReadTx) {
			totalReadCount *= 10;
			remoteColdCount *= 10;
		}

		// Local cold
		int localColdCount = totalReadCount - localHotCount - remoteHotCount - remoteColdCount;
		
		// Write Count
		int writeCount = 0;
		
		if (isReadWriteTx) {
			writeCount = (int) (totalReadCount * WRITE_RATIO_IN_RW_TX);
		}
		
		
		// =====================
		// Generating Parameters
		// =====================
		
		// Set read count
		paramList.add(totalReadCount);

		// Choose local hot records
		chooseHotData(paramList, mainPartition, localHotCount);

		// Choose local cold records
		chooseColdData(paramList, mainPartition, localColdCount);
		
		// Choose remote records
		if (isDistributedTx) {
			// randomly choose hot data from other partitions
			int[] partitionHotCount = new int[NUM_PARTITIONS];
			partitionHotCount[mainPartition] = 0;
			
			for (int i = 0; i < remoteHotCount; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition, rvg);
				partitionHotCount[remotePartition]++;
			}
			
			for (int i = 0; i < NUM_PARTITIONS; i++)
				chooseHotData(paramList, i, partitionHotCount[i]);

			// randomly choose cold data from other partitions
			int[] partitionColdCount = new int[NUM_PARTITIONS];
			partitionColdCount[mainPartition] = 0;

			for (int i = 0; i < remoteColdCount; i++) {
				int remotePartition = randomChooseOtherPartition(mainPartition, rvg);
				partitionColdCount[remotePartition]++;
			}

			for (int i = 0; i < NUM_PARTITIONS; i++)
				chooseColdData(paramList, i, partitionColdCount[i]);
		}
		
		// Set write count
		paramList.add(writeCount);

		// Choose write records
		if (writeCount > 0) {
			// A read-write tx must at least update one hot record.
			paramList.add(paramList.get(1));
			
			// Choose some item randomly from the rest of items
			Object[] writeIds = randomlyChooseInParams(paramList, 2,
					totalReadCount + 1, writeCount - 1);
			for (Object id : writeIds)
				paramList.add(id);

			// set the update value
			for (int i = 0; i < writeCount; i++)
				paramList.add(rvg.nextDouble() * 100000);
		}

		return paramList.toArray(new Object[0]);
	}

//	private void updateSessionUser(RandomValueGenerator rvg) {
//
//		// simulate the workload changing
//		int rteNum = wid;
//		if (System.currentTimeMillis() - startTime >= 4 * 60 * 1000) {
//			// 0 - 100
//			sessionUser = rteNum + 100;
//		} else if (System.currentTimeMillis() - startTime >= 3 * 60 * 1000) {
//			// 50 - 50
//			if (rvg.randomChooseFromDistribution(0.25, 0.75) == 1) {
//				sessionUser = rteNum + 100;
//			} else {
//				sessionUser = rteNum;
//			}
//		} else if (System.currentTimeMillis() - startTime >= 2 * 60 * 1000) {
//			// 50 - 50
//			if (rvg.randomChooseFromDistribution(0.5, 0.5) == 1) {
//				sessionUser = rteNum + 100;
//			} else {
//				sessionUser = rteNum;
//			}
//		} else if (System.currentTimeMillis() - startTime >= 1 * 60 * 1000) {
//			// 50 - 50
//			if (rvg.randomChooseFromDistribution(0.75, 0.25) == 1) {
//				sessionUser = rteNum + 100;
//			} else {
//				sessionUser = rteNum;
//			}
//		} else {
//			// 100 - 0
//			sessionUser = rteNum;
//		}
//		sessionUser = rteNum;
//		System.out.println("sessionUser: " + sessionUser);
//
//		// change the user every 10 rounds
//		if (sessionCountDown-- < 0) {
//			sessionCountDown = USER_SESSION_PERIOD;
//			sessionUser = rvg.number(0, USER_COUNT - 1);
//		}
//
//	}
	
	/**
	 * @param params
	 * @param startIdx the starting index (inclusive)
	 * @param endIdx the ending index (exclusive)
	 * @param count
	 * @return
	 */
	private Object[] randomlyChooseInParams(List<Object> params, int startIdx, int endIdx, int count) {
		// Create a clone
		Object[] tmps = new Object[endIdx - startIdx];
		for (int i = 0; i < tmps.length; i++)
			tmps[i] = params.get(startIdx + i);

		// Swap
		for (int times = 0; times < tmps.length * RANDOM_SWAP_FACTOR; times++) {
			int pos = random.nextInt(tmps.length - 1);

			Object tmp = tmps[pos];
			tmps[pos] = tmps[pos + 1];
			tmps[pos + 1] = tmp;
		}

		// Retrieve the first {count} results
		Object[] results = new Integer[count];
		for (int i = 0; i < count; i++)
			results[i] = tmps[i];

		return results;
	}
 
	private int randomChooseOtherPartition(int mainPartition, TpccValueGenerator rvg) {
		return ((mainPartition + rvg.number(1, NUM_PARTITIONS - 1)) % NUM_PARTITIONS);
	}

	private void chooseHotData(List<Object> paramList, int partition, int count) {
		int minMainPart = partition * DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(HOT_DATA_SIZE_PER_PART);
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPart + tmp;
			paramList.add(itemId);
		}

	}

	private void chooseColdData(List<Object> paramList, int partition, int count) {
		int minMainPartColdData = partition * DATA_SIZE_PER_PART + HOT_DATA_SIZE_PER_PART;
		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(COLD_DATA_SIZE_PER_PART);
		for (int i = 0; i < count; i++) {
			int tmp = rg.next(); // 1 ~ size
			int itemId = minMainPartColdData + tmp;
			paramList.add(itemId);
		}
		
		// Old code
//		int minMainPartColdData = partition * DATA_SIZE_PER_PART + HOT_DATA_SIZE_PER_PART;
//		RandomNonRepeatGenerator rg = new RandomNonRepeatGenerator(DATA_PER_USER);
//		for (int i = 0; i < count; i++) {
//			int tmp = rg.next(); // 1 ~ size
//			int itemId = minMainPartColdData + sessionUser * DATA_PER_USER + tmp;
//
//			itemId = itemRandomMap.get(itemId);
//			paramList.add(itemId);
//		}
	}
}
