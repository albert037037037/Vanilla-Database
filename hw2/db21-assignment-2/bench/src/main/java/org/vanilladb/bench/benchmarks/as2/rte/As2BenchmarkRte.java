/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
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
package org.vanilladb.bench.benchmarks.as2.rte;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;

public class As2BenchmarkRte extends RemoteTerminalEmulator<As2BenchTransactionType> {
	
	private As2BenchmarkTxExecutor executor_Read;
	private As2BenchmarkTxExecutor executor_Update;

	
	public As2BenchmarkRte(SutConnection conn, StatisticMgr statMgr) {
		super(conn, statMgr);
		executor_Read = new As2BenchmarkTxExecutor(new As2ReadItemParamGen());
		executor_Update = new As2BenchmarkTxExecutor(new As2UpdateItemPriceParamGen());
	}
	
	protected As2BenchTransactionType getNextTxType() {
		double r = Math.random();
		if(r > As2BenchConstants.RATIO_READ_UPDATE) {
			return As2BenchTransactionType.UPDATE_ITEM_PRICE;
		}
		else {
			return As2BenchTransactionType.READ_ITEM;
		}
	}
	
	protected As2BenchmarkTxExecutor getTxExeutor(As2BenchTransactionType type) {
		if(type == As2BenchTransactionType.READ_ITEM)
			return executor_Read;
		else if(type == As2BenchTransactionType.UPDATE_ITEM_PRICE);
			return executor_Update;
	}
}
