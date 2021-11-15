package org.elasql.bench.server.procedure.tpart.micro;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.benchmarks.micro.MicrobenchTransactionType;

public class MicrobenchStoredProcFactory implements TPartStoredProcedureFactory {
	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (MicrobenchTransactionType.fromProcedureId(pid)) {
		case MICRO_TXN:
			sp = new MicroTxnProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("Procedure " + MicrobenchTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}
}
