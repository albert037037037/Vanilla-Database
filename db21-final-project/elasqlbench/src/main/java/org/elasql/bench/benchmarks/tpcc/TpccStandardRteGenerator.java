package org.elasql.bench.benchmarks.tpcc;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.tpcc.rte.ElasqlTpccRte;
import org.elasql.bench.util.ElasqlBenchProperties;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class TpccStandardRteGenerator implements TpccRteGenerator {
	
private static Logger logger = Logger.getLogger(TpccStandardRteGenerator.class.getName());
	
	private static final double SKEW_RATIO;
	private static final int HOT_RTE_END_ID;
	private static final int HOTSPOT_WAREHOUSE_COUNT = 20;
	
	static {
		SKEW_RATIO = ElasqlBenchProperties.getLoader().getPropertyAsDouble(
				TpccStandardRteGenerator.class.getName() + ".SKEW_RATIO", 0.0);
		HOT_RTE_END_ID = (int) (BenchmarkerParameters.NUM_RTES * SKEW_RATIO);
	}
	
	private int nodeId;
	
	private int startWid;
	private int nextWidOffset = 0, nextDid = 1;
	
	private int rtesTargetHotspot = 0;
	
	public TpccStandardRteGenerator(int nodeId) {
		this.startWid = nodeId * ElasqlTpccConstants.WAREHOUSE_PER_PART + 1;
		this.nodeId = nodeId;
		
		if (HOT_RTE_END_ID > 0 && logger.isLoggable(Level.INFO))
			logger.info("TPC-C uses hot-spot workloads (first " + HOT_RTE_END_ID + " RTEs are hot)");
	}
	
	@Override
	public int getNumOfRTEs() {
		return BenchmarkerParameters.NUM_RTES;
	}

	@Override
	public RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		RemoteTerminalEmulator<TpccTransactionType> rte;
		
		// Hotspot workloads
		// Note: We assume that there is a client process for each server process.
		// Idea: Make each client pin some RTEs on one of warehouses on server 0.
		// Implementation:
		// - Client 0 distributes its RTEs evenly to warehouses of server 0.
		// - Client X (X != 0) pins its first Y RTEs to warehouse X of server 0.
		// - Y decides how hot server 0 is.
		if (nodeId == 0) {
			rte = selectNormally(conn, statMgr);
		} else {
			if (rtesTargetHotspot < HOT_RTE_END_ID) { // Y
				rte = selectHotspot(conn, statMgr, nodeId % HOTSPOT_WAREHOUSE_COUNT + 1);
				rtesTargetHotspot++;
				if (rtesTargetHotspot == HOT_RTE_END_ID) {
					nextDid = 1;
				}
			} else {
				rte = selectNormally(conn, statMgr);
			}
		}
		
		return rte;
	}
	
	private RemoteTerminalEmulator<TpccTransactionType> selectNormally(SutConnection conn, StatisticMgr statMgr) {
		RemoteTerminalEmulator<TpccTransactionType> rte =
				new ElasqlTpccRte(conn, statMgr, startWid + nextWidOffset, nextDid);
		
		// Find the next id
		nextWidOffset++;
		if (nextWidOffset >= ElasqlTpccConstants.WAREHOUSE_PER_PART) {
			nextWidOffset = 0;
			nextDid++;
			if (nextDid > TpccConstants.DISTRICTS_PER_WAREHOUSE) {
				nextDid = 1;
			}
		}
		
		return rte;
	}
	
	private RemoteTerminalEmulator<TpccTransactionType> selectHotspot(SutConnection conn, StatisticMgr statMgr, int hotspotWarehouse) {
		RemoteTerminalEmulator<TpccTransactionType> rte =
				new ElasqlTpccRte(conn, statMgr, hotspotWarehouse, nextDid);
		
		// Find the next id
		nextDid++;
		if (nextDid > TpccConstants.DISTRICTS_PER_WAREHOUSE) {
			nextDid = 1;
		}
		
		return rte;
	}
}
