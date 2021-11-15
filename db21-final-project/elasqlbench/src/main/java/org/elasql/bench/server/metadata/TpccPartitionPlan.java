package org.elasql.bench.server.metadata;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccConstants;
import org.elasql.server.Elasql;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.sql.Constant;

/**
 * Partitions each table on warehouse id.
 */
public class TpccPartitionPlan extends PartitionPlan {

	public boolean isFullyReplicated(PrimaryKey key) {
		return key.getTableName().equals("item");
	}

	public static Integer getWarehouseId(PrimaryKey key) {
		// For other tables, partitioned by wid
		Constant widCon;
		switch (key.getTableName()) {
		case "warehouse":
			widCon = key.getVal("w_id");
			break;
		case "district":
			widCon = key.getVal("d_w_id");
			break;
		case "stock":
			widCon = key.getVal("s_w_id");
			break;
		case "customer":
			widCon = key.getVal("c_w_id");
			break;
		case "history":
			widCon = key.getVal("h_c_w_id");
			break;
		case "orders":
			widCon = key.getVal("o_w_id");
			break;
		case "new_order":
			widCon = key.getVal("no_w_id");
			break;
		case "order_line":
			widCon = key.getVal("ol_w_id");
			break;
		default:
			return null;
		}
		
		return (Integer) widCon.asJavaVal();
	}
	
	public int numOfWarehouses() {
		return ElasqlTpccConstants.WAREHOUSE_PER_PART * PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public int getPartition(int wid) {
		return (wid - 1) / ElasqlTpccConstants.WAREHOUSE_PER_PART;
	}
	
	@Override
	public int getPartition(PrimaryKey key) {
		// If is item table, return self node id
		// (items are fully replicated over all partitions)
		if (key.getTableName().equals("item"))
			return Elasql.serverId();
		
		Integer wid = getWarehouseId(key);
		if (wid != null) {
			return getPartition(wid);
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
	public String toString() {
		return String.format("TPC-C range partition (each range has %d warehouses)", ElasqlTpccConstants.WAREHOUSE_PER_PART);
	}

	@Override
	public PartitioningKey getPartitioningKey(PrimaryKey key) {
		PartitioningKey partKey;
		
		switch (key.getTableName()) {
		case "warehouse":
			partKey = PartitioningKey.fromPrimaryKey(key, "w_id");
			break;
		case "district":
			partKey = PartitioningKey.fromPrimaryKey(key, "d_w_id");
			break;
		case "stock":
			partKey = PartitioningKey.fromPrimaryKey(key, "s_w_id");
			break;
		case "customer":
			partKey = PartitioningKey.fromPrimaryKey(key, "c_w_id");
			break;
		case "history":
			partKey = PartitioningKey.fromPrimaryKey(key, "h_c_w_id");
			break;
		case "orders":
			partKey = PartitioningKey.fromPrimaryKey(key, "o_w_id");
			break;
		case "new_order":
			partKey = PartitioningKey.fromPrimaryKey(key, "no_w_id");
			break;
		case "order_line":
			partKey = PartitioningKey.fromPrimaryKey(key, "ol_w_id");
			break;
		default:
			throw new RuntimeException("Unknown table " + key.getTableName());
		}

		return partKey;
	}
}
