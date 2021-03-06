/*******************************************************************************
 * Copyright 2016, 2017 elasql.org contributors
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

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.server.param.tpce.TpceSchemaBuilderParamHelper;
import org.vanilladb.core.server.VanillaDb;

public class TpceSchemaBuilderProc extends AllExecuteProcedure<TpceSchemaBuilderParamHelper> {
	private static Logger logger = Logger.getLogger(TpceSchemaBuilderProc.class.getName());
	
	public TpceSchemaBuilderProc(long txNum) {
		super(txNum, new TpceSchemaBuilderParamHelper());
	}

	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// Do nothing
		// XXX: We should lock those tables
		// localWriteTables.addAll(Arrays.asList(paramHelper.getTableNames()));
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.FINE))
			logger.info("Create schema for tpce testbed...");
		
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, getTransaction());
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, getTransaction());

	}
}
