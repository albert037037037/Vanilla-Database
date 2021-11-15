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
package org.elasql.bench.benchmarks.tpce;

import java.util.concurrent.atomic.AtomicLong;

import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.tpce.TpceConstants;
import org.vanilladb.bench.benchmarks.tpce.data.TpceDataManager;

public class ElasqlTpceDataManager extends TpceDataManager {
	
	private final AtomicLong nextTradeId = new AtomicLong(0);
	
	// Scale according the number of machines
	private final int nodeId;
	
	public ElasqlTpceDataManager(int nodeId) {
		super(TpceConstants.CUSTOMER_COUNT * PartitionMetaMgr.NUM_PARTITIONS,
				TpceConstants.COMPANY_COUNT * PartitionMetaMgr.NUM_PARTITIONS,
				TpceConstants.SECURITY_COUNT * PartitionMetaMgr.NUM_PARTITIONS);
		this.nodeId = nodeId;
	}
	
	public long getNextTradeId() {
		// XXX: If we can get the number of client nodes, we can use another way to generate
		return nextTradeId.getAndIncrement() + nodeId * 100_000_000;
	}

}