package org.elasql.bench.benchmarks.ycsb.rte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.PeriodicalJob;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
import org.vanilladb.bench.benchmarks.ycsb.rte.YcsbLatestGenerator;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.RandomValueGenerator;

/**
 * Parameter format: [1, read count, (read id array), write count,
 * (write id array), (write value array), insert count = 0]<br/>
 * <br/>
 * Single-table does not support insertions.
 * 
 * @author yslin
 */
public class SingleTableGoogleParamGen implements TxParamGenerator<YcsbTransactionType> {
	private static Logger logger = Logger.getLogger(SingleTableGoogleParamGen.class.getName());
	
	
	private static final double RW_TX_RATE = ElasqlYcsbConstants.RW_TX_RATE;
	private static final double DIST_TX_RATE = ElasqlYcsbConstants.DIST_TX_RATE;

	private static final boolean USE_DYNAMIC_RECORD_COUNT = ElasqlYcsbConstants.USE_DYNAMIC_RECORD_COUNT;
	private static final int TOTAL_RECORD_COUNT = ElasqlYcsbConstants.TX_RECORD_COUNT;
	private static final int REMOTE_RECORD_COUNT = (int) (ElasqlYcsbConstants.TX_RECORD_COUNT * 
			ElasqlYcsbConstants.REMOTE_RECORD_RATIO);
	private static final int RECORD_COUNT_MEAN = ElasqlYcsbConstants.RECORD_COUNT_MEAN;
	private static final int RECORD_COUNT_STD = ElasqlYcsbConstants.RECORD_COUNT_STD;
	
	// How many rounds that the global skew moves from the left to the right
	private static final int GLOBAL_SKEW_REPEAT = 3;
	private static final AtomicReference<TwoSidedSkewGenerator> TWO_SIDED_ZIP_TEMPLATE;
	private static final AtomicReference<YcsbLatestGenerator> ZIP_TEMPLATE;
	
	private static final int NUM_PARTITIONS = PartitionMetaMgr.NUM_PARTITIONS;
	private static final int DATABASE_SIZE = ElasqlYcsbConstants.INIT_RECORD_PER_PART * NUM_PARTITIONS;
	
	private static final double[][] WORKLOAD;
	private static final AtomicLong GLOBAL_START_TIME = new AtomicLong(0);
	
	// To delay replaying the workload (in milliseconds)
	private static final long DELAY_START_TIME = 90_000;
	
	static {
		WORKLOAD = ElasqlYcsbConstants.loadGoogleWorkloadTrace(PartitionMetaMgr.NUM_PARTITIONS);
		TWO_SIDED_ZIP_TEMPLATE = new AtomicReference<TwoSidedSkewGenerator>(
				new TwoSidedSkewGenerator(DATABASE_SIZE, ElasqlYcsbConstants.ZIPFIAN_PARAMETER));
		ZIP_TEMPLATE = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.INIT_RECORD_PER_PART,
						ElasqlYcsbConstants.ZIPFIAN_PARAMETER));
		
		if (logger.isLoggable(Level.INFO)) {
			String recordStr = "";
			if (USE_DYNAMIC_RECORD_COUNT) {
				recordStr = String.format("use dynamic record count with mean = %d and std = %d",
						RECORD_COUNT_MEAN, RECORD_COUNT_STD);
			} else {
				recordStr = String.format("%d records/tx, %d remote records/dist. tx",
						TOTAL_RECORD_COUNT, REMOTE_RECORD_COUNT);
			}
			logger.info(String.format("Use single-table Google YCSB generators "
					+ "(Read-write tx ratio: %f, distributed tx ratio: %f, "
					+ "%s, data size: %d, google trace file: %s, google trace length: %d)",
					RW_TX_RATE, DIST_TX_RATE, recordStr, DATABASE_SIZE,
					ElasqlYcsbConstants.GOOGLE_TRACE_FILE,
					ElasqlYcsbConstants.GOOGLE_TRACE_LENGTH));
		}
		
		// Debug: trace the current replay time
		new PeriodicalJob(5000, BenchmarkerParameters.BENCHMARK_INTERVAL, new Runnable() {
			@Override
			public void run() {
				// Wait for the start time set
				if (GLOBAL_START_TIME.get() == 0) {
					return;
				}
				
				int replayPoint = getCurrentReplayPoint();

				if (replayPoint >= 0 && replayPoint < WORKLOAD.length) {
					System.out.println(String.format("Replaying. Current replay point: %d", replayPoint));
				} else {
					System.out.println(String.format("Not replaying. Current replay point: %d", replayPoint));
				}
			}
		}).start();
	}
	
	private static int getCurrentReplayPoint() {
		long startTime = GLOBAL_START_TIME.get();
		if (startTime == 0) {
			// Update by compare-and-set
			startTime = System.nanoTime();
			if (!GLOBAL_START_TIME.compareAndSet(0, startTime)) {
				startTime = GLOBAL_START_TIME.get();
			}
		}
		long elapsedTime = (System.nanoTime() - startTime) / 1_000_000; // ns -> ms
		return (int) ((elapsedTime - DELAY_START_TIME) / 1000);
	}
	
	private TwoSidedSkewGenerator twoSidedZipGenerator;
	private YcsbLatestGenerator zipfianGenerator;
	
	public SingleTableGoogleParamGen() {
		this.twoSidedZipGenerator = new TwoSidedSkewGenerator(TWO_SIDED_ZIP_TEMPLATE.get());
		this.zipfianGenerator = new YcsbLatestGenerator(ZIP_TEMPLATE.get());
	}
	
	public static void main(String[] args) {
		SingleTableGoogleParamGen gen = new SingleTableGoogleParamGen();
		for (int i = 0; i < 10; i++)
			System.out.println(Arrays.toString(gen.generateParameter()));
	}

	@Override
	public YcsbTransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		
		// Check the current time point
		int replayPoint = getCurrentReplayPoint();
		boolean isReplaying = false;
		if (replayPoint >= 0 && replayPoint < WORKLOAD.length) {
			isReplaying = true;
		}

		// Decide the types of transactions
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0);
		boolean isDistTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0);
		if (NUM_PARTITIONS < 2 || !isReplaying)
			isDistTx = false;
		
		// Select a partition based on the distribution of the workload at the given time
		int mainPartId;
		if (isReplaying) { // Replay time
			mainPartId = rvg.randomChooseFromDistribution(WORKLOAD[replayPoint]);
		} else { // Non-replay time
			mainPartId = rvg.number(0, NUM_PARTITIONS - 1);
		}

		// Generate parameters
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(1); // dbtype = 1 (single-table)
		
		// Decide the number of records
		int totalRecordCount = TOTAL_RECORD_COUNT;
		int remoteRecordCount = REMOTE_RECORD_COUNT;
		if (USE_DYNAMIC_RECORD_COUNT) {
			double zeroMeanRandom = rvg.rng().nextGaussian();
			int randomCount = (int) (RECORD_COUNT_MEAN + zeroMeanRandom * RECORD_COUNT_STD);
			if (randomCount <= 1)
				randomCount = 2;
			if (randomCount > 50)
				randomCount = 50;
			
			totalRecordCount = randomCount;
			remoteRecordCount = randomCount / 2;
		}
		int localRecordCount = totalRecordCount;
		if (isDistTx) {
			localRecordCount -= remoteRecordCount;
		}
		
		// Read count
		paramList.add(totalRecordCount);
		
		// Read ids
		ArrayList<Long> ids = new ArrayList<Long>();
		
		// Local reads
		chooseRecordsInPart(mainPartId, localRecordCount, ids);
		
		// Remote reads
		if (isDistTx && remoteRecordCount > 0)
			chooseGlobalRecords(replayPoint, remoteRecordCount, ids);
		
		// Add the ids to the param list
		for (Long id : ids)
			paramList.add(id);
		
		// For read-write transactions
		if (isReadWriteTx) {
			// Write count
			paramList.add(totalRecordCount);
			
			// Write ids
			for (Long id : ids)
				paramList.add(id);
			
			// Write values
			for (int i = 0; i < totalRecordCount; i++)
				paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
			// Insert count
			// Single-table does not support insertion
			paramList.add(0);
		} else {
			// Write count
			paramList.add(0);
			// Insert count
			paramList.add(0);
		}
		
		return paramList.toArray(new Object[paramList.size()]);
	}
	
	private void chooseRecordsInPart(int partId, int count, ArrayList<Long> ids) {
		for (int i = 0; i < count; i++) {
			long id = chooseKeyInPart(partId);
			while (!ids.add(id))
				id = chooseKeyInPart(partId);
		}
	}
	
	private void chooseGlobalRecords(int replayTime, int count, ArrayList<Long> ids) {
		// Choose the center
		int center = DATABASE_SIZE / 2;
		if (replayTime >= 0 && replayTime < WORKLOAD.length) {
			// Note that it might be overflowed here.
			// The center of the 2-sided distribution changes
			// as the time increases. It moves from 0 to DATA_SIZE
			// and bounces back when it hits the end of the range. 
			int windowSize = WORKLOAD.length / GLOBAL_SKEW_REPEAT;
			int timeOffset = replayTime % (2 * windowSize);
			if (timeOffset >= windowSize)
				timeOffset = 2 * windowSize - timeOffset;
			center = DATABASE_SIZE / windowSize;
			center *= ((timeOffset % windowSize) + 1); 
		}
		
		// Use a global Zipfian distribution to select records
		for (int i = 0; i < count; i++) {
			long id = twoSidedZipGenerator.nextValue(center);
			while (!ids.add(id))
				id = twoSidedZipGenerator.nextValue(center);
		}
	}
	
	private long chooseKeyInPart(int partId) {
		long partStartId = partId * ElasqlYcsbConstants.INIT_RECORD_PER_PART;
		long offset = zipfianGenerator.nextValue();
		return partStartId + offset;
	}
}
