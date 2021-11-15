package org.elasql.bench.server.procedure.tpart.ycsb;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.benchmarks.ycsb.YcsbTransactionType;

public class TpartYcsbStoredProcFactory implements TPartStoredProcedureFactory {
	private static final int SP_DOING_REPLICATION = -1234;
	
	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		if (pid == SP_DOING_REPLICATION) {
			sp = new TpartYcsbProc(txNum, true);
			return sp;
		}
		
		switch (YcsbTransactionType.fromProcedureId(pid)) {
			case YCSB:
				sp = new TpartYcsbProc(txNum, false);
				break;
			default:
				throw new UnsupportedOperationException("The benchmarker does not recognize procedure " + pid + "");
		}
		return sp;
	}
}
