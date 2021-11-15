package org.vanilladb.bench.benchmarks.ycsb.rte;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.BenchProperties;
import org.vanilladb.bench.util.RandomValueGenerator;

public class YcsbParamGen implements TxParamGenerator<YcsbTransactionType> {

	private static final double RW_TX_RATE;
	private static final double SKEW_PARAMETER;
	
	private static final AtomicInteger GLOBAL_COUNTERS = new AtomicInteger(0);
	
	static {
		RW_TX_RATE = BenchProperties.getLoader()
				.getPropertyAsDouble(YcsbParamGen.class.getName() + ".RW_TX_RATE", 0.15);
		SKEW_PARAMETER = BenchProperties.getLoader()
				.getPropertyAsDouble(YcsbParamGen.class.getName() + ".SKEW_PARAMETER", 0.99);
	}
	
	private static int getNextInsertId() {
		int id = GLOBAL_COUNTERS.getAndIncrement();
		
		return id + YcsbConstants.NUM_RECORDS;
	}
	
	private YcsbLatestGenerator latestRandom;
	private RandomValueGenerator rvg = new RandomValueGenerator();

	public YcsbParamGen() {
		latestRandom = new YcsbLatestGenerator(YcsbConstants.NUM_RECORDS, SKEW_PARAMETER);
	}
	
	// a main application for debugging
	public static void main(String[] args) {
		YcsbParamGen executor = new YcsbParamGen();
		
		System.out.println("Parameters:");
		System.out.println("Read Write Tx Rate: " + RW_TX_RATE);
		System.out.println("Skew Parameter: " + SKEW_PARAMETER);
		System.out.println();

		for (int i = 0; i < 1000; i++) {
			Object[] params = executor.generateParameter();
			System.out.println(Arrays.toString(params));
		}
	}
	
	@Override
	public YcsbTransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}
	
	@Override
	public Object[] generateParameter() {

		ArrayList<Object> paramList = new ArrayList<Object>();
		
		// ================================
		// Decide the types of transactions
		// ================================
		
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0) ? true : false;
		
		// =====================
		// Generating Parameters
		// =====================
		
		if (isReadWriteTx) {
			int readWriteId = chooseARecord();
			int insertId = getNextInsertId();
			
			// Read count
			paramList.add(1);
			
			// Read ids (in integer)
			paramList.add(readWriteId);
			
			// Write count
			paramList.add(1);
			
			// Write ids (in integer)
			paramList.add(readWriteId);
			
			// Write values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
			// Insert count
			paramList.add(1);
			
			// Insert ids (in integer)
			paramList.add(insertId);
			
			// Insert values
			paramList.add(rvg.randomAString(YcsbConstants.CHARS_PER_FIELD));
			
		} else {
			int rec1Id = chooseARecord();
			int rec2Id = rec1Id;
			while (rec1Id == rec2Id)
				rec2Id = chooseARecord();
			
			// Read count
			paramList.add(2);
			
			// Read ids (in integer)
			paramList.add(rec1Id);
			paramList.add(rec2Id);
			
			// Write count
			paramList.add(0);
			
			// Insert count
			paramList.add(0);
		}
		
		return paramList.toArray(new Object[0]);
	}

	private int chooseARecord() {
		return (int) latestRandom.nextValue();
	}
}
