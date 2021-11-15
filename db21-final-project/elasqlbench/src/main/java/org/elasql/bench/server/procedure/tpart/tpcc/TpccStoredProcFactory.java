package org.elasql.bench.server.procedure.tpart.tpcc;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;

public class TpccStoredProcFactory implements TPartStoredProcedureFactory {

	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		switch (TpccTransactionType.fromProcedureId(pid)) {
		case NEW_ORDER:
			sp = new NewOrderProc(txNum);
			break;
		case PAYMENT:
			sp = new PaymentProc(txNum);
			break;
		default:
			sp = null;
		}
		return sp;
	}
}
