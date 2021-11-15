package org.elasql.bench.benchmarks.ycsb;

import org.elasql.bench.benchmarks.ycsb.rte.ElasqlYcsbRte;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.ycsb.YcsbBenchmark;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ElasqlYcsbBenchmark extends YcsbBenchmark {
	
	private int nodeId;
	private int rteId = 0;
	
	public ElasqlYcsbBenchmark(int nodeId) {
		this.nodeId = nodeId;
	}
	
	@Override
	public RemoteTerminalEmulator<YcsbTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		return new ElasqlYcsbRte(conn, statMgr, nodeId, rteId++);
	}
}
