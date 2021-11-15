package org.elasql.bench.server.metadata;

import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

public class TpcePartitionPlan extends PartitionPlan {

	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		return false;
	}

	@Override
	public int getPartition(PrimaryKey key) {
		// XXX: Currently, we partition on the first field, but there
		// must be a better choice.
		Constant val = key.getVal(0);
		return Math.abs(val.hashCode() % PartitionMetaMgr.NUM_PARTITIONS);
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
}
