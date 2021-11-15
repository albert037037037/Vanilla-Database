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

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.bench.server.param.tpce.TradeOrderParamHelper;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.VarcharConstant;

// XXX: Simplified version
public class TradeOrderProc extends CalvinStoredProcedure<TradeOrderParamHelper> {

	String acctName, custFName, custLName, taxId, brokerName, exchId, sName, statusId;
	long brokerId, custId, coId;
	int taxStatus, custTier, typeIsMarket, typeIsSell;
	double marketPrice;
	
	private PrimaryKey cusAcctKey, customerKey, brokerKey, securityKey,
			lastTradeKey, tradeTypeKey, tradeKey, tradeHistoryKey;
	
	public TradeOrderProc(long txNum) {
		super(txNum, new TradeOrderParamHelper());
	}

	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		PrimaryKeyBuilder builder;
		
		/***************** Construct Read Keys *******************/
		// Customer Account
		builder = new PrimaryKeyBuilder("customer_account");
		builder.addFldVal("ca_id", new BigIntConstant(paramHelper.getAcctId()));
		cusAcctKey = builder.build();
		analyzer.addReadKey(cusAcctKey);

		// Customer
		builder = new PrimaryKeyBuilder("customer");
		builder.addFldVal("c_id", new BigIntConstant(paramHelper.getCustomerId()));
		customerKey = builder.build();
		analyzer.addReadKey(customerKey);

		// Broker
		builder = new PrimaryKeyBuilder("broker");
		builder.addFldVal("b_id", new BigIntConstant(paramHelper.getBrokerId()));
		brokerKey = builder.build();
		analyzer.addReadKey(brokerKey);

		// Security
		builder = new PrimaryKeyBuilder("security");
		builder.addFldVal("s_symb", new VarcharConstant(paramHelper.getSymbol()));
		securityKey = builder.build();
		analyzer.addReadKey(securityKey);

		// Last Trade
		builder = new PrimaryKeyBuilder("last_trade");
		builder.addFldVal("lt_s_symb",
				new VarcharConstant(paramHelper.getSymbol()));
		lastTradeKey = builder.build();
		analyzer.addReadKey(lastTradeKey);

		// Trade Type
		builder = new PrimaryKeyBuilder("trade_type");
		builder.addFldVal("tt_id",
				new VarcharConstant(paramHelper.getTradeTypeId()));
		tradeTypeKey = builder.build();
		analyzer.addReadKey(tradeTypeKey);

		
		/***************** Construct Write Keys *******************/
		// Insert new trade
		builder = new PrimaryKeyBuilder("trade");
		builder.addFldVal("t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeKey = builder.build();
		analyzer.addInsertKey(tradeKey);

		// Insert new history
		builder = new PrimaryKeyBuilder("trade_history");
		builder.addFldVal("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		tradeHistoryKey = builder.build();
		analyzer.addInsertKey(tradeHistoryKey);
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		CachedRecord rec = readings.get(cusAcctKey);
		acctName = (String) rec.getVal("ca_name").asJavaVal();
		brokerId = (Long) rec.getVal("ca_b_id").asJavaVal();
		custId = (Long) rec.getVal("ca_c_id").asJavaVal();
		taxStatus = (Integer) rec.getVal("ca_tax_st").asJavaVal();
		
		rec = readings.get(customerKey);
		custFName = (String) rec.getVal("c_f_name").asJavaVal();
		custLName = (String) rec.getVal("c_l_name").asJavaVal();
		custTier = (Integer) rec.getVal("c_tier").asJavaVal();
		taxId = (String) rec.getVal("c_tax_id").asJavaVal();
		
		rec = readings.get(brokerKey);
		brokerName = (String) rec.getVal("b_name").asJavaVal();
		
		rec = readings.get(securityKey);
		coId = (Long) rec.getVal("s_co_id").asJavaVal();
		exchId = (String) rec.getVal("s_ex_id").asJavaVal();
		sName = (String) rec.getVal("s_name").asJavaVal();
		
		rec = readings.get(lastTradeKey);
		marketPrice = (Double) rec.getVal("lt_price").asJavaVal();
		
		rec = readings.get(tradeTypeKey);
		typeIsMarket = (Integer) rec.getVal("tt_is_mrkt").asJavaVal();
		typeIsSell = (Integer) rec.getVal("tt_is_sell").asJavaVal();
		
		if (typeIsMarket == 1) {
			statusId = "A";
		} else {
			statusId = "B";
		}
		
		// INSERT INTO trade (t_id, t_dts, t_st_id, t_tt_id, t_is_cash,
		// t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_trade_price,
		// t_chrg, t_comm, t_tax, t_lifo) VALUES (...)
		long currentTime = System.currentTimeMillis();
		
		Map<String, Constant> fldVals = new HashMap<String, Constant>();
		fldVals.put("t_id", new BigIntConstant(paramHelper.getTradeId()));
		fldVals.put("t_dts", new BigIntConstant(currentTime));
		fldVals.put("t_st_id", new VarcharConstant(statusId));
		fldVals.put("t_tt_id", new VarcharConstant(paramHelper.getTradeTypeId()));
		fldVals.put("t_is_cash", new IntegerConstant(1));
		fldVals.put("t_s_symb", new VarcharConstant(paramHelper.getSymbol()));
		fldVals.put("t_qty", new IntegerConstant(paramHelper.getTradeQty()));
		fldVals.put("t_bid_price", new DoubleConstant(marketPrice));
		fldVals.put("t_ca_id", new BigIntConstant(paramHelper.getAcctId()));
		fldVals.put("t_exec_name", new VarcharConstant("exec_name"));
		fldVals.put("t_trade_price", new DoubleConstant(paramHelper.getTradePrice()));
		fldVals.put("t_chrg", new DoubleConstant(0.0));
		fldVals.put("t_comm", new DoubleConstant(0.0));
		fldVals.put("t_tax", new DoubleConstant(0.0));
		fldVals.put("t_lifo", new IntegerConstant(1));
		insert(tradeKey, fldVals);

		// INSERT INTO trade_history (th_t_id, th_dts, th_st_id) VALUES (...)
		fldVals = new HashMap<String, Constant>();
		fldVals.put("th_t_id", new BigIntConstant(paramHelper.getTradeId()));
		fldVals.put("th_dts", new BigIntConstant(currentTime));
		fldVals.put("th_st_id", new VarcharConstant(statusId));
		insert(tradeHistoryKey, fldVals);
	}

}
