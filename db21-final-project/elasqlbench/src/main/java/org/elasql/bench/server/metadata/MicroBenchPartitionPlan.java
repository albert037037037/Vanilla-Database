package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.micro.ElasqlMicrobenchConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class MicroBenchPartitionPlan extends PartitionPlan {
	
	public Integer getItemId(PrimaryKey key) {
		Constant iidCon = key.getVal("i_id");
		if (iidCon != null) {
			return (Integer) iidCon.asJavaVal();
		} else {
			return null;
		}
	}
	
	public boolean isFullyReplicated(PrimaryKey key) {
		if (key.getVal("i_id") != null) {
			return false;
		} else {
			return true;
		}
	}
	
	public int getPartition(int iid) {
		return (iid - 1) / ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
	}
	
	public int getPartition(PrimaryKey key) {
		Integer iid = getItemId(key);
		if (iid != null) {
			return getPartition(iid);
		} else {
			// Fully replicated
			return Elasql.serverId();
		}
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
		if (key.getTableName().equals("item"))
			return PartitioningKey.fromPrimaryKey(key);
		throw new RuntimeException("Unknown table " + key.getTableName());
	}
}
