package org.elasql.bench.benchmarks.ycsb.rte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
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
public class SingleTableNormalParamGen implements TxParamGenerator<YcsbTransactionType> {
	private static Logger logger = Logger.getLogger(SingleTableNormalParamGen.class.getName());
	
	private static final double RW_TX_RATE = ElasqlYcsbConstants.RW_TX_RATE;
	private static final double DIST_TX_RATE = ElasqlYcsbConstants.DIST_TX_RATE;
	private static final int TOTAL_RECORD_COUNT = ElasqlYcsbConstants.TX_RECORD_COUNT;
	private static final int REMOTE_RECORD_COUNT = (int) (ElasqlYcsbConstants.TX_RECORD_COUNT * 
			ElasqlYcsbConstants.REMOTE_RECORD_RATIO);
	
	private static final AtomicReference<YcsbLatestGenerator> GEN_TEMPLATE;
	
	static {
		if (ElasqlYcsbConstants.USE_DYNAMIC_RECORD_COUNT)
			throw new RuntimeException(String.format("%s does not support dynamic record count",
					SingleTableNormalParamGen.class.getName()));
		
		GEN_TEMPLATE = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(ElasqlYcsbConstants.INIT_RECORD_PER_PART,
						ElasqlYcsbConstants.ZIPFIAN_PARAMETER));
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Use single-table normal YCSB generators "
					+ "(Read-write tx ratio: %f, distributed tx ratio: %f, "
					+ "%d records/tx, %d remote records/dist. tx)",
					RW_TX_RATE, DIST_TX_RATE, TOTAL_RECORD_COUNT, REMOTE_RECORD_COUNT));
	}
	
	private int numOfPartitions;
	private YcsbLatestGenerator generator;
	
	public SingleTableNormalParamGen(int numOfPartitions) {
		this.numOfPartitions = numOfPartitions;
		this.generator = new YcsbLatestGenerator(GEN_TEMPLATE.get());
	}
	
	public static void main(String[] args) {
		SingleTableNormalParamGen gen = new SingleTableNormalParamGen(5);
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

		// Decide the types of transactions
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0);
		boolean isDistTx = (rvg.randomChooseFromDistribution(DIST_TX_RATE, 1 - DIST_TX_RATE) == 0);
		if (numOfPartitions < 2)
			isDistTx = false;

		// Generate parameters
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(1); // dbtype = 1 (single-table)
		
		// Select a partition
		int mainPartId = rvg.number(0, numOfPartitions - 1);
		
		// Read count
		paramList.add(TOTAL_RECORD_COUNT);
		
		// Read ids
		int localReadCount = TOTAL_RECORD_COUNT - REMOTE_RECORD_COUNT;
		ArrayList<Long> ids = new ArrayList<Long>();
		for (int i = 0; i < TOTAL_RECORD_COUNT; i++) {
			int partId = mainPartId;
			
			// Choose a remote partition
			if (isDistTx && i >= localReadCount) {
				while (partId == mainPartId)
					partId = rvg.number(0, numOfPartitions - 1);
			}
			
			// Choose a key from the partition
			Long id = chooseKeyInPart(partId);
			while (ids.contains(id))
				id = chooseKeyInPart(partId);
			paramList.add(id);
			ids.add(id);
		}
		
		if (isReadWriteTx) {
			// Write count
			paramList.add(TOTAL_RECORD_COUNT);
			
			// Write ids
			for (Long id : ids)
				paramList.add(id);
			
			// Write values
			for (int i = 0; i < TOTAL_RECORD_COUNT; i++)
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
	
	private long chooseKeyInPart(int partId) {
		long partStartId = partId * ElasqlYcsbConstants.INIT_RECORD_PER_PART;
		long offset = generator.nextValue();
		return partStartId + offset;
	}
}
