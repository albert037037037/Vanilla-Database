package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.benchmarks.tpcc.rte.ElasqlTpccRte;
import org.elasql.bench.server.metadata.migration.TpccBeforePartPlan;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class TpccMigrationTestRteGenerator implements TpccRteGenerator {
	
	// For migration test
	private static final int RTE_PER_NORMAL_WAREHOUSE = 5;
	private static final int RTE_PER_COLD_WAREHOUSE = 1;
	private static final int RTE_PER_HOT_WAREHOUSE = 50;
	
	private static final int NUM_HOT_PARTS = TpccBeforePartPlan.NUM_HOT_PARTS;
	private static final int NUM_COLD_PARTS = NUM_HOT_PARTS * (TpccBeforePartPlan.HOT_WAREHOUSE_PER_HOT_PART - 1);
	
	private static final int TOTAL_RETS_FOR_NORMALS_PER_NODE = RTE_PER_NORMAL_WAREHOUSE * 
			TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART;
	private static final int TOTAL_RETS_FOR_COLDS_PER_NODE = RTE_PER_COLD_WAREHOUSE * 
			TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART;
	
	private static final int NUM_OF_NORMAL_WAREHOUSE = TpccBeforePartPlan.MAX_NORMAL_WID;
	
	public static void main(String[] args) {
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			TpccMigrationTestRteGenerator gen = new TpccMigrationTestRteGenerator(nodeId);
			System.out.print(String.format("Node %d with %d RTEs: ", nodeId, gen.getNumOfRTEs()));
			for (int rteId = 0; rteId < gen.getNumOfRTEs(); rteId++) {
				gen.createRte(null, null);
				System.out.print(String.format("[W: %d, D: %d] ", gen.warehouseId, gen.districtId));
			}
			System.out.println();
		}
	}
	
	private int nodeId;
	private int nextRteId = 0;
	
	public TpccMigrationTestRteGenerator(int nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public int getNumOfRTEs() {
		if (nodeId < NUM_HOT_PARTS) {
			return TOTAL_RETS_FOR_NORMALS_PER_NODE + RTE_PER_HOT_WAREHOUSE;
		} else if (nodeId < NUM_HOT_PARTS + NUM_COLD_PARTS)
			return TOTAL_RETS_FOR_COLDS_PER_NODE + RTE_PER_HOT_WAREHOUSE;
		else
			return TOTAL_RETS_FOR_NORMALS_PER_NODE;
	}
	
	int warehouseId;
	int districtId;

	@Override
	public RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		
		if (nodeId < NUM_HOT_PARTS) {
			if (nextRteId < TOTAL_RETS_FOR_NORMALS_PER_NODE) { // for normal warehouses
				warehouseId = nextRteId / RTE_PER_NORMAL_WAREHOUSE +
						TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART * nodeId + 1;
				districtId = nextRteId % RTE_PER_NORMAL_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
			} else { // for hot warehouses
				int offset = nextRteId - TOTAL_RETS_FOR_NORMALS_PER_NODE;
				warehouseId = offset / RTE_PER_HOT_WAREHOUSE + NUM_OF_NORMAL_WAREHOUSE + nodeId + 1;
				districtId = offset % RTE_PER_HOT_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
			}
		} else if (nodeId < NUM_HOT_PARTS + NUM_COLD_PARTS) {
			if (nextRteId < TOTAL_RETS_FOR_COLDS_PER_NODE) { // for normal warehouses
				warehouseId = nextRteId / RTE_PER_COLD_WAREHOUSE +
						TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART * nodeId + 1;
				districtId = nextRteId % RTE_PER_COLD_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
			} else { // for hot warehouses
				int offset = nextRteId - TOTAL_RETS_FOR_COLDS_PER_NODE;
				warehouseId = offset / RTE_PER_HOT_WAREHOUSE + NUM_OF_NORMAL_WAREHOUSE + nodeId + 1;
				districtId = offset % RTE_PER_HOT_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
			}
		} else {
			// for normal warehouses
			warehouseId = nextRteId / RTE_PER_NORMAL_WAREHOUSE +
					TpccBeforePartPlan.NORMAL_WAREHOUSE_PER_PART * nodeId + 1;
			districtId = nextRteId % RTE_PER_NORMAL_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
		}
		
		nextRteId++;
		return new ElasqlTpccRte(conn, statMgr, warehouseId, districtId);
	}
}
