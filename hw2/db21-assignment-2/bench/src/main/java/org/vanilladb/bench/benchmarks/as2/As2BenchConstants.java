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
package org.vanilladb.bench.benchmarks.as2;

import org.vanilladb.bench.util.BenchProperties;

public class As2BenchConstants {

	public static final int NUM_ITEMS;
	public static final int READ_COUNT;
	public static final double RATIO_READ_UPDATE;
	
	static {
		NUM_ITEMS = BenchProperties.getLoader().getPropertyAsInteger(
				As2BenchConstants.class.getName() + ".NUM_ITEMS", 100000);
		READ_COUNT = BenchProperties.getLoader().getPropertyAsInteger(
				As2BenchConstants.class.getName() + ".TOTAL_READ_COUNT", 10);
		RATIO_READ_UPDATE = BenchProperties.getLoader().getPropertyAsDouble(
				As2BenchConstants.class.getName() + ".READ_WRITE_TX_RATE", 0.3);
	}

	public static final int MIN_IM = 1;
	public static final int MAX_IM = 10000;
	public static final double MIN_PRICE = 1.00;
	public static final double MAX_PRICE = 100.00;
	public static final int MIN_I_NAME = 14;
	public static final int MAX_I_NAME = 24;
	public static final int MIN_I_DATA = 26;
	public static final int MAX_I_DATA = 50;
	public static final int MONEY_DECIMALS = 2;

}
