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
package org.vanilladb.bench.benchmarks.as2.rte.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.benchmarks.as2.As2BenchConstants;
import org.vanilladb.bench.remote.SutResultSet;
import org.vanilladb.bench.remote.jdbc.VanillaDbJdbcResultSet;
import org.vanilladb.bench.rte.jdbc.JdbcJob;
import org.vanilladb.bench.server.param.as2.ReadItemProcParamHelper;
import org.vanilladb.bench.server.param.as2.UpdateItemPriceProcParamHelper;

public class UpdateItemPriceTxnJdbcJob implements JdbcJob {
	private static Logger logger = Logger.getLogger(UpdateItemPriceTxnJdbcJob.class
			.getName());
	
	@Override
	public SutResultSet execute(Connection conn, Object[] pars) throws SQLException {

		UpdateItemPriceProcParamHelper paramHelper = new UpdateItemPriceProcParamHelper();
		paramHelper.prepareParameters(pars);
		
		// Output message
		StringBuilder outputMsg = new StringBuilder("[");
		
		// Execute logic
		try {
			Statement statement = conn.createStatement();
			ResultSet rs = null;
			
			// SELECT
			for (int i = 0; i < paramHelper.getUpdateCount(); i++) {
				int iid = paramHelper.getUpdateItemId(i);
				double addPrice = paramHelper.getUpdatePrice(i);
				String sql = "SELECT i_price FROM item WHERE i_id = " + iid;
				rs = statement.executeQuery(sql);
				rs.beforeFirst();
				if (rs.next()) {
					double price = rs.getDouble("i_price");
					double itemNewPrice = price + addPrice;
					if (itemNewPrice > As2BenchConstants.MAX_PRICE)
						itemNewPrice = As2BenchConstants.MIN_PRICE;
					String sql_update = "UPDATE item SET i_price = " + itemNewPrice + " WHERE i_id = " + iid;
					statement.executeUpdate(sql_update);
					outputMsg.append(String.format("item %d's price change from %f to %f ! ", iid, price, itemNewPrice));
				} else
					throw new RuntimeException("cannot find the record with i_id = " + iid);
				rs.close();
			}
			
			conn.commit();
			
			outputMsg.deleteCharAt(outputMsg.length() - 2);
			outputMsg.append("]");
			
			return new VanillaDbJdbcResultSet(true, outputMsg.toString());
		} catch (Exception e) {
			if (logger.isLoggable(Level.WARNING))
				logger.warning(e.toString());
			return new VanillaDbJdbcResultSet(false, "");
		}
	}
}
