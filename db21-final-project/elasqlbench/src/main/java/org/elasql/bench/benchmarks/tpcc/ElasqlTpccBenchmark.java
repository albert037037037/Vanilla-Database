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
package org.elasql.bench.benchmarks.tpcc;

import org.elasql.bench.server.metadata.TpccPartitionPlan;
import org.elasql.bench.server.metadata.migration.TpccBeforePartPlan;
import org.elasql.bench.server.metadata.migration.scaleout.TpccScaleoutBeforePartPlan;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccBenchmark;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;

public class ElasqlTpccBenchmark extends TpccBenchmark {
	
	private static final TpccPartitionPlan partPlan;
			
	static {
		switch (ElasqlTpccConstants.PARTITION_STRATEGY) {
		case MGCRAB_SCALING_OUT:
			partPlan = new TpccScaleoutBeforePartPlan();
			break;
		case MGCRAB_CONSOLIDATION:
			partPlan = new TpccBeforePartPlan();
			break;
		default:
			partPlan = new TpccPartitionPlan();
			break;
		}
	}
	
	public static TpccPartitionPlan getPartitionPlan() {
		return partPlan;
	}
	
	public static int getNumOfWarehouses() {
		return partPlan.numOfWarehouses();
	}
	
	private TpccRteGenerator rteGenerator;
	
	public ElasqlTpccBenchmark(int nodeId) {
		switch (ElasqlTpccConstants.PARTITION_STRATEGY) {
		case MGCRAB_SCALING_OUT:
			rteGenerator = new TpccScaleoutTestRteGenerator(nodeId);
			break;
		case MGCRAB_CONSOLIDATION:
			rteGenerator = new TpccMigrationTestRteGenerator(nodeId);
			break;
		default:
			rteGenerator = new TpccStandardRteGenerator(nodeId);
			break;
		}
	}

	@Override
	public int getNumOfRTEs() {
		return rteGenerator.getNumOfRTEs();
	}
	
	@Override
	public RemoteTerminalEmulator<TpccTransactionType> createRte(SutConnection conn, StatisticMgr statMgr) {
		return rteGenerator.createRte(conn, statMgr);
	}
}
