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
package org.elasql.bench.benchmarks.micro;

import org.elasql.bench.util.ElasqlBenchProperties;

public class ElasqlMicrobenchConstants {

	public static final int NUM_ITEMS_PER_NODE;
	
	static {
		NUM_ITEMS_PER_NODE = ElasqlBenchProperties.getLoader().getPropertyAsInteger(
				ElasqlMicrobenchConstants.class.getName() + ".NUM_ITEMS_PER_NODE", 100000);
	}

}
