package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.util.ElasqlBenchProperties;

public class ElasqlTpccConstants {
	
	public enum TpccPartitionStategy { NORMAL, MGCRAB_SCALING_OUT, MGCRAB_CONSOLIDATION };
	
	// 1: Normal, 2: MgCrab scaling-out, 3: MgCrab consolidation
	public static final TpccPartitionStategy PARTITION_STRATEGY;
	public static final int WAREHOUSE_PER_PART;
	
	static {
		int strategy = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlTpccConstants.class.getName() + ".PARTITION_STRATEGY", 1);
		switch (strategy) {
		case 2:
			PARTITION_STRATEGY = TpccPartitionStategy.MGCRAB_SCALING_OUT;
			break;
		case 3:
			PARTITION_STRATEGY = TpccPartitionStategy.MGCRAB_CONSOLIDATION;
			break;
		default:
			PARTITION_STRATEGY = TpccPartitionStategy.NORMAL;
			break;
		}
		WAREHOUSE_PER_PART = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlTpccConstants.class.getName() + ".WAREHOUSE_PER_PART", 1);
	}
}
