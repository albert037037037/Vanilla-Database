package org.elasql.bench;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.micro.ElasqlMicroBenchmark;
import org.elasql.bench.benchmarks.tpcc.ElasqlTpccBenchmark;
import org.elasql.bench.benchmarks.tpce.ElasqlTpceBenchmark;
import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbBenchmark;
import org.elasql.bench.remote.sp.ElasqlBenchSpConnection;
import org.elasql.bench.remote.sp.ElasqlBenchSpDriver;
import org.elasql.remote.groupcomm.client.DirectMessageListener;
import org.elasql.server.Elasql;
import org.elasql.server.Elasql.ServiceType;
import org.vanilladb.bench.BenchTransactionType;
import org.vanilladb.bench.Benchmark;
import org.vanilladb.bench.BenchmarkerParameters;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.remote.SutDriver;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ElasqlBench implements DirectMessageListener {
	private static Logger logger = Logger.getLogger(ElasqlBench.class.getName());
	
	private int nodeId;
	private SutDriver driver;
	private Benchmark benchmarker;
	private StatisticMgr statMgr;
	private BlockingQueue<CheckDatabaseResult> checkDbResult;
	
	public ElasqlBench(int nodeId) {
		this.nodeId = nodeId;
		this.driver = newDriver(nodeId);
		this.benchmarker = newBenchmarker(nodeId);
		this.statMgr = newStatisticMgr(benchmarker, nodeId);
		this.checkDbResult = new ArrayBlockingQueue<CheckDatabaseResult>(1);
	}

	public void loadTestbed() {
		if (logger.isLoggable(Level.INFO))
			logger.info("loading the testbed of the benchmark...");

		try {
			SutConnection con = getConnection();
			benchmarker.executeLoadingProcedure(con);
		} catch (SQLException e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("Error: " + e.getMessage());
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("loading procedure finished.");
	}

	public void benchmark() {
		try {
			if (logger.isLoggable(Level.INFO))
				logger.info("checking the database on the server...");

			SutConnection conn = getConnection();
//			boolean result = checkDatabase(conn);
//
//			if (!result) {
//				if (logger.isLoggable(Level.SEVERE))
//					logger.severe("the database is not ready, please load the database again.");
//				return;
//			}

			if (logger.isLoggable(Level.INFO))
				logger.info("database check passed.");

			if (logger.isLoggable(Level.INFO))
				logger.info("creating " + BenchmarkerParameters.NUM_RTES + " emulators...");
			
			int rteCount = benchmarker.getNumOfRTEs();
			RemoteTerminalEmulator<?>[] emulators = new RemoteTerminalEmulator[rteCount];
			emulators[0] = benchmarker.createRte(conn, statMgr); // Reuse the connection
			for (int i = 1; i < emulators.length; i++)
				emulators[i] = benchmarker.createRte(getConnection(), statMgr);

			if (logger.isLoggable(Level.INFO))
				logger.info("waiting for connections...");

			// TODO: Do we still need this ?
			// Wait for connections
			Thread.sleep(1500);

			if (logger.isLoggable(Level.INFO))
				logger.info("start benchmarking.");

			// Start the execution of the RTEs
			for (int i = 0; i < emulators.length; i++)
				emulators[i].start();

			// Waits for the warming up finishes
			Thread.sleep(BenchmarkerParameters.WARM_UP_INTERVAL);

			if (logger.isLoggable(Level.INFO))
				logger.info("warm up period finished.");

			if (BenchmarkerParameters.PROFILING_ON_SERVER && nodeId == 0) {
				if (logger.isLoggable(Level.INFO))
					logger.info("starting the profiler on the server-side");
				
				benchmarker.startProfilingProcedure(getConnection());
			}

			if (logger.isLoggable(Level.INFO))
				logger.info("start recording results...");

			// notify RTEs for recording statistics
			for (int i = 0; i < emulators.length; i++)
				emulators[i].startRecordStatistic();

			// waiting
			Thread.sleep(BenchmarkerParameters.BENCHMARK_INTERVAL);

			if (logger.isLoggable(Level.INFO))
				logger.info("benchmark preiod finished. Stoping RTEs...");

			// benchmark finished
			for (int i = 0; i < emulators.length; i++)
				emulators[i].stopBenchmark();

			if (BenchmarkerParameters.PROFILING_ON_SERVER && nodeId == 0) {
				if (logger.isLoggable(Level.INFO))
					logger.info("stoping the profiler on the server-side");

				benchmarker.stopProfilingProcedure(getConnection());
			}

			// TODO: Do we need to 'join' ?
			// for (int i = 0; i < emulators.length; i++)
			// emulators[i].join();

			// Create a report
			statMgr.outputReport();

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			if (logger.isLoggable(Level.SEVERE))
				logger.severe("Error: " + e.getMessage());
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("benchmark process finished.");
	}

	@Override
	public void onReceivedDirectMessage(Object message) {
		if (message.getClass().equals(CheckDatabaseResult.class)) {
			checkDbResult.add((CheckDatabaseResult) message);
		} else {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("unknown p2p message: " + message);
		}
	}
	
	private SutDriver newDriver(int nodeId) {
		// Create a driver for connection
		switch (BenchmarkerParameters.CONNECTION_MODE) {
		case JDBC:
			throw new UnsupportedOperationException("ElaSQL does not support JDBC");
		case SP:
			return new ElasqlBenchSpDriver(nodeId, this);
		}
		return null;
	}
	
	private Benchmark newBenchmarker(int nodeId) {
		switch (BenchmarkerParameters.BENCH_TYPE) {
		case MICRO:
			return new ElasqlMicroBenchmark();
		case TPCC:
			return new ElasqlTpccBenchmark(nodeId);
		case TPCE:
			return new ElasqlTpceBenchmark(nodeId);
		case YCSB:
			return new ElasqlYcsbBenchmark(nodeId);
		}
		return null;
	}
	
	private StatisticMgr newStatisticMgr(Benchmark benchmarker, int nodeId) {
		Set<BenchTransactionType> txnTypes = benchmarker.getBenchmarkingTxTypes();
		String reportPostfix = benchmarker.getBenchmarkName();
		reportPostfix += String.format("-%d", nodeId);
		return new StatisticMgr(txnTypes, reportPostfix);
	}
	
	private SutConnection getConnection() throws SQLException {
		return driver.connectToSut();
	}
	
	private boolean checkDatabase(SutConnection conn) throws SQLException {
		// XXX: We haven't implement check procedure for other service types
		if (Elasql.SERVICE_TYPE != ServiceType.CALVIN) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning("skips checking procedures for " + Elasql.SERVICE_TYPE);
			return true;
		}
		
		if (nodeId == 0) {
			boolean result = benchmarker.executeDatabaseCheckProcedure(conn);
			
			if (logger.isLoggable(Level.INFO))
				logger.info("check finished. Sending the results to other client nodes...");
			
			ElasqlBenchSpConnection elasqlConn = (ElasqlBenchSpConnection) conn;
			for (int clientId = 1; clientId < elasqlConn.getClientCount(); clientId++)
				elasqlConn.sendDirectMessage(clientId, new CheckDatabaseResult(result));
			return result;
		} else {
			if (logger.isLoggable(Level.INFO))
				logger.info("waiting for the check result from the client 0...");
			
			CheckDatabaseResult dBResult = null;
			try {
				dBResult = checkDbResult.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			if (logger.isLoggable(Level.INFO))
				logger.info("received the result from the clinet 0");
			
			return dBResult.getResult();
		}
	}
}
