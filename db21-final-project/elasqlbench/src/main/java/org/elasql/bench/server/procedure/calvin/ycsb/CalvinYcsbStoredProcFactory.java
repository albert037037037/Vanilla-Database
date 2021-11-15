package org.elasql.bench.server.procedure.calvin.ycsb;

import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;

public class CalvinYcsbStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (YcsbTransactionType.fromProcedureId(pid)) {
		case SCHEMA_BUILDER:
			sp = new YcsbSchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new YcsbTestbedLoaderProc(txNum);
			break;
		case CHECK_DATABASE:
			sp = new YcsbCheckDatabaseProc(txNum);
			break;
		case YCSB:
			sp = new CalvinYcsbProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}
}
