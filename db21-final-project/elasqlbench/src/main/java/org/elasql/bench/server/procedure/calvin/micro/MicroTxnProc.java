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
package org.elasql.bench.server.procedure.calvin.micro;

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;

public class MicroTxnProc extends CalvinStoredProcedure<MicroTxnProcParamHelper> {

	private Map<PrimaryKey, Constant> writeConstantMap = new HashMap<PrimaryKey, Constant>();

	public MicroTxnProc(long txNum) {
		super(txNum, new MicroTxnProcParamHelper());
	}

	@Override
	public void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// set read keys
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			int iid = paramHelper.getReadItemId(idx);
			
			// create record key for reading
			PrimaryKey key = new PrimaryKey("item", "i_id", new IntegerConstant(iid));
			analyzer.addReadKey(key);
		}

		// set write keys
		for (int idx = 0; idx < paramHelper.getWriteCount(); idx++) {
			int iid = paramHelper.getWriteItemId(idx);
			double newPrice = paramHelper.getNewItemPrice(idx);
			
			// create record key for writing
			PrimaryKey key = new PrimaryKey("item", "i_id", new IntegerConstant(iid));
			analyzer.addUpdateKey(key);

			// Create key-value pairs for writing
			Constant c = new DoubleConstant(newPrice);
			writeConstantMap.put(key, c);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// SELECT i_name, i_price FROM items WHERE i_id = ...
		int idx = 0;
		for (CachedRecord rec : readings.values()) {
			paramHelper.setItemName((String) rec.getVal("i_name").asJavaVal(), idx);
			paramHelper.setItemPrice((double) rec.getVal("i_price").asJavaVal(), idx++);
		}

		// UPDATE items SET i_price = ... WHERE i_id = ...
		for (Map.Entry<PrimaryKey, Constant> pair : writeConstantMap.entrySet()) {
			CachedRecord rec = readings.get(pair.getKey());
			rec.setVal("i_price", pair.getValue());
			update(pair.getKey(), rec);
		}

	}
}
