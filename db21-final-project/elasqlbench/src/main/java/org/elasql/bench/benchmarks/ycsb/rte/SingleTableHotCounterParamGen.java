package org.elasql.bench.benchmarks.ycsb.rte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
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
public class SingleTableHotCounterParamGen implements TxParamGenerator<YcsbTransactionType> {
	private static Logger logger = Logger.getLogger(SingleTableHotCounterParamGen.class.getName());
	
	private static final double RW_TX_RATE = ElasqlYcsbConstants.RW_TX_RATE;
	private static final double DIST_TX_RATE = ElasqlYcsbConstants.DIST_TX_RATE;
	private static final int TX_RECORD_COUNT = ElasqlYcsbConstants.TX_RECORD_COUNT;
	private static final int TX_COLD_RECORD_COUNT = ElasqlYcsbConstants.TX_RECORD_COUNT - 1;
	
	private static final int HOT_COUNT_PER_PART = ElasqlYcsbConstants.HOT_COUNT_PER_PART;
	private static final double HOT_UPDATE_RATE_IN_RW_TX = ElasqlYcsbConstants.HOT_UPDATE_RATE_IN_RW_TX;
	private static final int COLD_COUNT_PER_PART = ElasqlYcsbConstants.INIT_RECORD_PER_PART - HOT_COUNT_PER_PART;
	
	static {
		if (ElasqlYcsbConstants.USE_DYNAMIC_RECORD_COUNT)
			throw new RuntimeException(String.format("%s does not support dynamic record count",
					SingleTableHotCounterParamGen.class.getName()));
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Use single-table hot-counter YCSB generators "
					+ "(Read-write tx ratio: %f, distributed tx ratio: %f, "
					+ "%d records/tx, %d cold records/tx, %d hot records/partition, "
					+ "%d cold records/partition)",
					RW_TX_RATE, DIST_TX_RATE, TX_RECORD_COUNT, TX_COLD_RECORD_COUNT,
					HOT_COUNT_PER_PART, COLD_COUNT_PER_PART));
	}
	
	private int numOfPartitions;
	private RandomValueGenerator rvg = new RandomValueGenerator();
	
	public SingleTableHotCounterParamGen(int numOfPartitions) {
		this.numOfPartitions = numOfPartitions;
	}
	
	public static void main(String[] args) {
		SingleTableHotCounterParamGen gen = new SingleTableHotCounterParamGen(5);
		for (int i = 0; i < 10; i++)
			System.out.println(Arrays.toString(gen.generateParameter()));
	}

	@Override
	public YcsbTransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}

	@Override
	public Object[] generateParameter() {
		// Decide the types of transactions
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0);
		boolean isDistTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0);
		if (numOfPartitions < 2)
			isDistTx = false;

		// Generate parameters
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(1); // dbtype = 1 (single-table)
		
		// Select a hot record from a partition
		int hotRecordPartId = rvg.number(0, numOfPartitions - 1);
		long hotRecordId = chooseHotRecordInPart(hotRecordPartId);
		
		// Select cold records from another partition (may be the same or not)
		ArrayList<Long> coldRecordIds = new ArrayList<Long>();
		int coldRecordPartId = hotRecordPartId;
		if (isDistTx) {
			int offset = rvg.number(1, numOfPartitions - 1);
			coldRecordPartId = (coldRecordPartId + offset) % numOfPartitions;
		}
		while (coldRecordIds.size() < TX_COLD_RECORD_COUNT) {
			long coldRecordId = chooseColdRecordInPart(coldRecordPartId);
			if (!coldRecordIds.contains(coldRecordId))
				coldRecordIds.add(coldRecordId);
		}
		
		// Add read Ids
		paramList.add(TX_RECORD_COUNT);
		paramList.add(hotRecordId);
		for (long coldRecordId : coldRecordIds)
			paramList.add(coldRecordId);
		
		// Add write Ids
		if (isReadWriteTx) {
			// Decides if there is a write for the hot record
			boolean hasHotUpdate = (rvg.randomChooseFromDistribution(
					HOT_UPDATE_RATE_IN_RW_TX, 1 - HOT_UPDATE_RATE_IN_RW_TX) == 0);
			
			// Write count
			if (hasHotUpdate)
				paramList.add(TX_RECORD_COUNT);
			else
				paramList.add(TX_COLD_RECORD_COUNT);
			
			// Write ids
			if (hasHotUpdate)
				paramList.add(hotRecordId);
			for (long coldRecordId : coldRecordIds)
				paramList.add(coldRecordId);
			
			// Write values
			if (hasHotUpdate)
				paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			for (int i = 0; i < TX_COLD_RECORD_COUNT; i++)
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
	
	private long chooseHotRecordInPart(int partId) {
		long partStartId = partId * ElasqlYcsbConstants.INIT_RECORD_PER_PART;
		long offset = rvg.number(1, HOT_COUNT_PER_PART);
		return partStartId + offset;
	}
	
	private long chooseColdRecordInPart(int partId) {
		long partStartId = partId * ElasqlYcsbConstants.INIT_RECORD_PER_PART;
		long offset = rvg.number(1, COLD_COUNT_PER_PART);
		return partStartId + HOT_COUNT_PER_PART + offset;
	}
}
