package org.elasql.bench.benchmarks.ycsb.rte;

import static org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.ENABLE_HOTSPOT;
import static org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.HOTSPOT_CHANGE_PERIOD;
import static org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.HOTSPOT_HOTNESS;
import static org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.INIT_RECORD_PER_PART;
import static org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.RW_TX_RATE;
import static org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.TENANTS_PER_PART;
import static org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants.TX_RECORD_COUNT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
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
public class SingleTableMultiTenantParamGen implements TxParamGenerator<YcsbTransactionType> {
	private static Logger logger = Logger.getLogger(SingleTableMultiTenantParamGen.class.getName());
	
	private static final int RECORDS_PER_PART = INIT_RECORD_PER_PART;
	private static final int RECORDS_PER_TENANT = RECORDS_PER_PART / TENANTS_PER_PART;
	
	private static final AtomicReference<YcsbLatestGenerator> GEN_TEMPLATE;
	
	// To delay replaying the workload (in milliseconds)
	private static final long DELAY_START_TIME = 120_000;
	private static final AtomicLong GLOBAL_START_TIME = new AtomicLong(0);
	
	private static boolean reporterEnabled = false;
	
	static {
		if (ElasqlYcsbConstants.USE_DYNAMIC_RECORD_COUNT)
			throw new RuntimeException(String.format("%s does not support dynamic record count",
					SingleTableMultiTenantParamGen.class.getName()));
		
		GEN_TEMPLATE = new AtomicReference<YcsbLatestGenerator>(
				new YcsbLatestGenerator(RECORDS_PER_TENANT, ElasqlYcsbConstants.ZIPFIAN_PARAMETER));
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Use single-table multi-tenant YCSB generators "
					+ "(Read-write tx ratio: %f, %d records/tx, %d tenants/partition, has hotspot? %s)",
					RW_TX_RATE, TX_RECORD_COUNT, TENANTS_PER_PART, ENABLE_HOTSPOT));
		
	}
	
	private static int getCurrentTimeOffset() {
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

	// Debug: trace the current hotspot
	private static void enableReporter(final int numberOfPartitions) {
		if (reporterEnabled)
			return;
		reporterEnabled = true;
		
		new PeriodicalJob(5000, BenchmarkerParameters.BENCHMARK_INTERVAL, new Runnable() {
			@Override
			public void run() {
				// Wait for the start time set
				if (GLOBAL_START_TIME.get() == 0) {
					return;
				}
				
				int timeOffset = getCurrentTimeOffset();

				if (ENABLE_HOTSPOT && timeOffset >= 0) {
					int hotspotPartId = (timeOffset / HOTSPOT_CHANGE_PERIOD) % numberOfPartitions;
					System.out.println(String.format("Time Offset: %d, hotspot part: %d",
							timeOffset, hotspotPartId));
				} else {
					System.out.println(String.format("Time Offset: %d, no hotspot", timeOffset));
				}
			}
		}).start();
	}
	
	public static void main(String[] args) {
		SingleTableMultiTenantParamGen gen = new SingleTableMultiTenantParamGen(4);
		for (int i = 0; i < 10; i++)
			System.out.println(Arrays.toString(gen.generateParameter()));
	}
	
	private int numOfPartitions;
	private YcsbLatestGenerator generator;
	
	public SingleTableMultiTenantParamGen(int numOfPartitions) {
		this.numOfPartitions = numOfPartitions;
		this.generator = new YcsbLatestGenerator(GEN_TEMPLATE.get());
		
		enableReporter(numOfPartitions);
	}

	@Override
	public YcsbTransactionType getTxnType() {
		return YcsbTransactionType.YCSB;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		
		// Check the current time point
		int timeOffset = getCurrentTimeOffset();

		// Decide the types of transactions
		boolean isReadWriteTx = (rvg.randomChooseFromDistribution(RW_TX_RATE, 1 - RW_TX_RATE) == 0);

		// Generate parameters
		ArrayList<Object> paramList = new ArrayList<Object>();
		paramList.add(1); // dbtype = 1 (single-table)
		
		// Select a partition
		int mainPartId = 0;
		if (ENABLE_HOTSPOT && timeOffset >= 0) {
			int hotspotPartId = (timeOffset / HOTSPOT_CHANGE_PERIOD) % numOfPartitions;
			
			// Choose a partition with respect to hotness
			if (rvg.rng().nextDouble() > HOTSPOT_HOTNESS) {
				mainPartId = rvg.number(0, numOfPartitions - 1);
				while (mainPartId == hotspotPartId)
					mainPartId = rvg.number(0, numOfPartitions - 1);
			} else {
				mainPartId = hotspotPartId;
			}
		} else {
			// Uniformly select a partition
			mainPartId = rvg.number(0, numOfPartitions - 1);
		}
		
		// Uniformly select a tenant inside the chosen partition
		int tenantId = rvg.number(mainPartId * TENANTS_PER_PART,
				(mainPartId + 1) * TENANTS_PER_PART - 1);
		
		// Read count
		paramList.add(TX_RECORD_COUNT);
		
		// Read ids
		ArrayList<Long> ids = new ArrayList<Long>();
		for (int i = 0; i < TX_RECORD_COUNT; i++) {
			// Choose a key from the tenant
			Long id = chooseKeyInTenant(tenantId);
			while (ids.contains(id))
				id = chooseKeyInTenant(tenantId);
			paramList.add(id);
			ids.add(id);
		}
		
		if (isReadWriteTx) {
			// Write count
			paramList.add(TX_RECORD_COUNT);
			
			// Write ids
			for (Long id : ids)
				paramList.add(id);
			
			// Write values
			for (int i = 0; i < TX_RECORD_COUNT; i++)
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
	
	private long chooseKeyInTenant(int tenantId) {
		long tanentStartId = tenantId * RECORDS_PER_TENANT;
		long offset = generator.nextValue();
		return tanentStartId + offset;
	}
}
