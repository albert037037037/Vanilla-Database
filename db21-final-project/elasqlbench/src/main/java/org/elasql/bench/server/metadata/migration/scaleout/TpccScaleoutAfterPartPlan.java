package org.elasql.bench.server.metadata.migration.scaleout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.storage.metadata.PartitionMetaMgr;

public class TpccScaleoutAfterPartPlan extends TpccScaleoutBeforePartPlan implements Serializable {
	
	private static final long serialVersionUID = 20181031001l;

	/**
	 * E.g. 3 Nodes:
	 * - Node 0 {1~10}
	 * - Node 1 {11~20}
	 * - Node 2 {21}
	 */
	public int getPartition(int wid) {
		if (wid <= MAX_NORMAL_WID)
			return (wid - 1) / NORMAL_WAREHOUSE_PER_PART;
		else
			return (wid - MAX_NORMAL_WID - 1) % NUM_EMPTY_PARTS + (NUM_HOT_PARTS + NUM_NORMAL_PARTS);
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