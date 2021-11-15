/*******************************************************************************
 * Copyright 2016, 2018 elasql.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.elasql.bench.server.procedure.calvin.tpce;

import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.vanilladb.bench.benchmarks.tpce.TpceTransactionType;

public class TpceStoredProcFactory implements CalvinStoredProcedureFactory {

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		CalvinStoredProcedure<?> sp;
		switch (TpceTransactionType.fromProcedureId(pid)) {
		case SCHEMA_BUILDER:
			sp = new TpceSchemaBuilderProc(txNum);
			break;
		case TESTBED_LOADER:
			sp = new TpceTestbedLoaderProc(txNum);
			break;
		case TRADE_ORDER:
			sp = new TradeOrderProc(txNum);
			break;
		case TRADE_RESULT:
			sp = new TradeResultProc(txNum);
			break;
		default:
			throw new UnsupportedOperationException("Procedure " + TpceTransactionType.fromProcedureId(pid) + " is not supported for now");
		}
		return sp;
	}

}
