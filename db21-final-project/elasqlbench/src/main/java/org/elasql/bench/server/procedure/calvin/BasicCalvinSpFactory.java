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
package org.elasql.bench.server.procedure.calvin;

import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.procedure.calvin.CalvinStoredProcedureFactory;
import org.vanilladb.bench.ControlTransactionType;

public class BasicCalvinSpFactory implements CalvinStoredProcedureFactory {
	
	private CalvinStoredProcedureFactory underlayerFactory;
	
	public BasicCalvinSpFactory(CalvinStoredProcedureFactory underlayerFactory) {
		this.underlayerFactory = underlayerFactory;
	}

	@Override
	public CalvinStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		ControlTransactionType txnType = ControlTransactionType.fromProcedureId(pid);
		if (txnType != null) {
			switch (txnType) {
			case START_PROFILING:
				return new StartProfilingProc(txNum);
			case STOP_PROFILING:
				return new StopProfilingProc(txNum);
			}
		}
		return underlayerFactory.getStoredProcedure(pid, txNum);
	}
}
