package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class YcsbSingleTablePartitionPlan extends PartitionPlan {
	
	public static int getYcsbId(PrimaryKey key) {
		Constant idCon = key.getVal("ycsb_id");
		if (idCon == null)
			throw new IllegalArgumentException("does not recongnize " + key);
		return Integer.parseInt((String) idCon.asJavaVal());
	}
	
	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		return false;
	}
	
	@Override
	public int getPartition(PrimaryKey key) {
		int ycsbId = getYcsbId(key);
		return (ycsbId - 1) / ElasqlYcsbConstants.INIT_RECORD_PER_PART;
	}

	@Override
	public PartitionPlan getBasePlan() {
		return this;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		new UnsupportedOperationException();
	}

	@Override
	public PartitioningKey getPartitioningKey(PrimaryKey key) {
		return PartitioningKey.fromPrimaryKey(key);
	}
	
	@Override
	public String toString() {
		return String.format("YCSB range partition (each range has %d records)", ElasqlYcsbConstants.INIT_RECORD_PER_PART);
	}
}
