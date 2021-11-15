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

import java.util.ArrayList;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.benchmarks.as2.As2BenchTransactionType;
import org.vanilladb.bench.rte.TxParamGenerator;
import org.vanilladb.bench.util.BenchProperties;
import org.vanilladb.bench.util.RandomValueGenerator;

public class As2UpdateItemPriceParamGen implements TxParamGenerator<As2BenchTransactionType> {
	
	// Read Counts
	private static final int TOTAL_READ_COUNT;

	static {
		TOTAL_READ_COUNT = BenchProperties.getLoader()
				.getPropertyAsInteger(As2BenchConstants.class.getName() + ".TOTAL_READ_COUNT", 10);
	}

	@Override
	public As2BenchTransactionType getTxnType() {
		return As2BenchTransactionType.UPDATE_ITEM_PRICE;
	}

	@Override
	public Object[] generateParameter() {
		RandomValueGenerator rvg = new RandomValueGenerator();
		ArrayList<Object> paramList = new ArrayList<Object>();

		// Set read count
		paramList.add(TOTAL_READ_COUNT);
		// Set read id and update price
		for (int i = 0; i < TOTAL_READ_COUNT; i++){
			paramList.add(rvg.number(1, As2BenchConstants.NUM_ITEMS));
			double d = Math.round(rvg.randomDoubleIncrRange(0.0, 5.01, 0.1) * 10) / 10.0;
			paramList.add(d);
		}

		return paramList.toArray(new Object[0]);
	}
}
