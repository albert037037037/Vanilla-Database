package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.benchmarks.tpcc.rte.ElasqlTpccRte;
import org.elasql.bench.server.metadata.migration.scaleout.TpccScaleoutBeforePartPlan;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class TpccScaleoutTestRteGenerator implements TpccRteGenerator {
	
	// Each node should has the same number of RTEs
	// Each node handles the workloads to a server partition
	private static final int RTE_PER_NORMAL_WAREHOUSE = 6;
	private static final int RTE_PER_HOT_WAREHOUSE = RTE_PER_NORMAL_WAREHOUSE * 
			TpccScaleoutBeforePartPlan.NORMAL_WAREHOUSE_PER_PART;
	private static final int NUM_OF_NON_EMPTY_PARTS = 
			TpccScaleoutBeforePartPlan.NUM_HOT_PARTS + TpccScaleoutBeforePartPlan.NUM_NORMAL_PARTS;
	
	public static void main(String[] args) {
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			TpccScaleoutTestRteGenerator gen = new TpccScaleoutTestRteGenerator(nodeId);
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
	
	public TpccScaleoutTestRteGenerator(int nodeId) {
		this.nodeId = nodeId;
	}

	@Override
	public int getNumOfRTEs() {
		return RTE_PER_NORMAL_WAREHOUSE * 
				TpccScaleoutBeforePartPlan.NORMAL_WAREHOUSE_PER_PART;
	}
	
	int warehouseId;
	int districtId;

	@Override
	public RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		if (nodeId < NUM_OF_NON_EMPTY_PARTS) {
			warehouseId = nextRteId / RTE_PER_NORMAL_WAREHOUSE +
					TpccScaleoutBeforePartPlan.NORMAL_WAREHOUSE_PER_PART * nodeId + 1;
			districtId = nextRteId % RTE_PER_NORMAL_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
		} else {
			warehouseId = nextRteId / RTE_PER_HOT_WAREHOUSE +
					TpccScaleoutBeforePartPlan.MAX_NORMAL_WID + (nodeId - NUM_OF_NON_EMPTY_PARTS) + 1;
			districtId = nextRteId % RTE_PER_HOT_WAREHOUSE % TpccConstants.DISTRICTS_PER_WAREHOUSE + 1;
		}
		nextRteId++;
		return new ElasqlTpccRte(conn, statMgr, warehouseId, districtId);
	}
}
