package org.elasql.bench.server.metadata.migration.scaleout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class TpccScaleoutBeforePartPlan extends TpccPartitionPlan {
	
	// 1 to 1
	public static final int NUM_HOT_PARTS = 1;
	public static final int HOT_WAREHOUSE_PER_HOT_PART = 1;
	
	// 3 to 3
//	public static final int NUM_HOT_PARTS = 3;
//	public static final int HOT_WAREHOUSE_PER_HOT_PART = 1;
	
	// 3 to 6
//	public static final int NUM_HOT_PARTS = 3;
//	public static final int HOT_WAREHOUSE_PER_HOT_PART = 2;
	
	// we wish to distribute the hot warehouses to the empty partitions
	// each empty partition should get one hot warehouse.
	public static final int NUM_EMPTY_PARTS = NUM_HOT_PARTS * HOT_WAREHOUSE_PER_HOT_PART;
	public static final int NUM_NORMAL_PARTS = PartitionMetaMgr.NUM_PARTITIONS - NUM_HOT_PARTS - NUM_EMPTY_PARTS;

	public static final int NORMAL_WAREHOUSE_PER_PART = 10;
	
	public static final int MAX_NORMAL_WID = NORMAL_WAREHOUSE_PER_PART
			* (NUM_HOT_PARTS + NUM_NORMAL_PARTS);
	
	public int numOfWarehouses() {
		return MAX_NORMAL_WID +
				HOT_WAREHOUSE_PER_HOT_PART * NUM_HOT_PARTS;
	}
	
	/**
	 * E.g. 3 Nodes:
	 * - Node 0 {1~10,21}
	 * - Node 1 {11~20}
	 * - Node 2 {}
	 */
	public int getPartition(int wid) {
		if (wid <= MAX_NORMAL_WID)
			return (wid - 1) / NORMAL_WAREHOUSE_PER_PART;
		else
			return (wid - MAX_NORMAL_WID - 1) % NUM_HOT_PARTS;
	}
	
	@Override
	public String toString() {
		Map<Integer, List<Integer>> wids = new HashMap<Integer, List<Integer>>();
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++)
			wids.put(partId, new ArrayList<Integer>());
		
		for (int wid = 1; wid <= numOfWarehouses(); wid++)
			wids.get(getPartition(wid)).add(wid);

		StringBuilder sb = new StringBuilder("TPC-C Plan: { Warehouse Ids:");
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
			sb.append(String.format("Part %d:[", partId));
			
			for (Integer wid : wids.get(partId))
				sb.append(String.format("%d, ", wid));
			sb.delete(sb.length() - 2, sb.length());
			
			sb.append("], ");
		}
		sb.delete(sb.length() - 2, sb.length());
		sb.append("}");
		
		return sb.toString();
	}
}