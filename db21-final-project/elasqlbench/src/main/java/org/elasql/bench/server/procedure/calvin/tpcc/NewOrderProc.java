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
package org.elasql.bench.server.procedure.calvin.tpcc;

import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.benchmarks.tpcc.ElasqlTpccBenchmark;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.server.param.tpcc.NewOrderProcParamHelper;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.DoubleConstant;
import org.vanilladb.core.sql.IntegerConstant;


/**
 * Entering a new order is done in a single database transaction with the
 * following steps:<br>
 * 1. Create an order header, comprised of: <br>
 * - 2 row selections with data retrieval <br>
 * - 1 row selections with data retrieval and update<br>
 * - 2 row insertions <br>
 * 2. Order a variable number of items (average ol_cnt = 10), comprised of:
 * <br>
 * - (1 * ol_cnt) row selections with data retrieval <br>
 * - (1 * ol_cnt) row selections with data retrieval and update <br>
 * - (1 * ol_cnt) row insertions <br>
 * 
 * @author yslin
 *
 */
public class NewOrderProc extends CalvinStoredProcedure<NewOrderProcParamHelper> {

	// hard code the next order id
	// TODO: This should be retrieve from district table
	// Note that this is not thread-safe, but it is fine
	// for this to be used in prepareKeys()
	private static int[] distrOIds;
	static {
		int warehouseCount = ElasqlTpccBenchmark.getNumOfWarehouses();
		distrOIds = new int[warehouseCount * TpccConstants.DISTRICTS_PER_WAREHOUSE + 100];
		for (int i = 0; i < distrOIds.length; i++)
			distrOIds[i] = 3001;
	}
	private int fakeOid;
	
	public static int getNextOrderId(int wid, int did)  {
		return distrOIds[(wid - 1) * 10 + did - 1];
	}

	// Record keys for retrieving data
	private PrimaryKey warehouseKey, districtKey, customerKey;
	private PrimaryKey orderKey, newOrderKey;
	// a {itemKey, stockKey, orderLineKey} per order line
	private PrimaryKey[][] orderLineKeys = new PrimaryKey[15][3];

	// SQL Constants
	Constant widCon, didCon, cidCon, oidCon;

	public NewOrderProc(long txNum) {
		super(txNum, new NewOrderProcParamHelper());
	}

	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		PrimaryKeyBuilder builder;

		// Construct constant from parameters
		widCon = new IntegerConstant(paramHelper.getWid());
		didCon = new IntegerConstant(paramHelper.getDid());
		cidCon = new IntegerConstant(paramHelper.getCid());

		// hard code the next order id
		// TODO: This should be retrieve from district table
		int index = (paramHelper.getWid() - 1) * 10 + paramHelper.getDid() - 1;
		fakeOid = distrOIds[index];
		distrOIds[index] = fakeOid + 1;
		oidCon = new IntegerConstant(fakeOid);

		// =================== Keys for steps 1 ===================

		// SELECT ... FROM warehouse WHERE w_id = wid
		builder = new PrimaryKeyBuilder("warehouse");
		builder.addFldVal("w_id", widCon);
		warehouseKey = builder.build();
		analyzer.addReadKey(warehouseKey);

		// SELECT ... FROM district WHERE d_w_id = wid AND d_id = did
		builder = new PrimaryKeyBuilder("district");
		builder.addFldVal("d_w_id", widCon);
		builder.addFldVal("d_id", didCon);
		districtKey = builder.build();
		analyzer.addReadKey(districtKey);

		// UPDATE ... WHERE d_w_id = wid AND d_id = did
		analyzer.addUpdateKey(districtKey);

		// SELECT ... WHERE c_w_id = wid AND c_d_id = did AND c_id = cid
		builder = new PrimaryKeyBuilder("customer");
		builder.addFldVal("c_w_id", widCon);
		builder.addFldVal("c_d_id", didCon);
		builder.addFldVal("c_id", cidCon);
		customerKey = builder.build();
		analyzer.addReadKey(customerKey);

		// INSERT INTO orders (o_id, o_w_id, o_d_id, ...) VALUES (nextOId, wid,
		// did, ...)
		builder = new PrimaryKeyBuilder("orders");
		builder.addFldVal("o_w_id", widCon);
		builder.addFldVal("o_d_id", didCon);
		builder.addFldVal("o_id", oidCon);
		orderKey = builder.build();
		analyzer.addInsertKey(orderKey);

		// INSERT INTO new_order (no_o_id, no_w_id, no_d_id) VALUES
		// (nextOId, wid, did)
		builder = new PrimaryKeyBuilder("new_order");
		builder.addFldVal("no_w_id", widCon);
		builder.addFldVal("no_d_id", didCon);
		builder.addFldVal("no_o_id", oidCon);
		newOrderKey = builder.build();
		analyzer.addInsertKey(newOrderKey);

		// =================== Keys for steps 2 ===================
		int orderLineCount = paramHelper.getOlCount();
		int[][] items = paramHelper.getItems();

		// For each order line
		for (int i = 0; i < orderLineCount; i++) {
			// initialize variables
			int olIId = items[i][0];
			int olSupplyWId = items[i][1];
			Constant olIIdCon = new IntegerConstant(olIId);
			Constant supWidCon = new IntegerConstant(olSupplyWId);
			Constant olNumCon = new IntegerConstant(i + 1);

			// SELECT ... FROM item WHERE i_id = olIId
			builder = new PrimaryKeyBuilder("item");
			builder.addFldVal("i_id", olIIdCon);
			orderLineKeys[i][0] = builder.build();
			analyzer.addReadKey(orderLineKeys[i][0]);

			// SELECT ... FROM stock WHERE s_i_id = olIId AND s_w_id =
			// olSupplyWId
			builder = new PrimaryKeyBuilder("stock");
			builder.addFldVal("s_i_id", olIIdCon);
			builder.addFldVal("s_w_id", supWidCon);
			orderLineKeys[i][1] = builder.build();
			analyzer.addReadKey(orderLineKeys[i][1]);

			// UPDATE ... WHERE s_i_id = olIId AND s_w_id = olSupplyWId
			analyzer.addUpdateKey(orderLineKeys[i][1]);

			// INSERT INTO order_line (ol_o_id, ol_w_id, ol_d_id, ol_number,
			// ...)
			// VALUES (nextOId, wid, did, i, ...)
			builder = new PrimaryKeyBuilder("order_line");
			builder.addFldVal("ol_o_id", oidCon);
			builder.addFldVal("ol_d_id", didCon);
			builder.addFldVal("ol_w_id", widCon);
			builder.addFldVal("ol_number", olNumCon);
			orderLineKeys[i][2] = builder.build();
			analyzer.addInsertKey(orderLineKeys[i][2]);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		CachedRecord rec = null;
		Map<String, Constant> fldVals = null;

		// SELECT w_tax FROM warehouse WHERE w_id = wid
		rec = readings.get(warehouseKey);
		paramHelper.setWTax((Double) rec.getVal("w_tax").asJavaVal());

		// SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = wid AND d_id =
		// did
		rec = readings.get(districtKey);
		paramHelper.setdTax((Double) rec.getVal("d_tax").asJavaVal());
		// XXX: This should be used for next order id
		rec.getVal("d_next_o_id").asJavaVal();

		// UPDATE district SET d_next_o_id = (nextOId + 1) WHERE d_w_id = wid
		// AND d_id = did
		rec = readings.get(districtKey);
		rec.setVal("d_next_o_id", new IntegerConstant(fakeOid + 1));
		update(districtKey, rec);

		// SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = wid
		// AND
		// c_d_id = did AND c_id = cid
		rec = readings.get(customerKey);
		paramHelper.setcDiscount((Double) rec.getVal("c_discount").asJavaVal());
		paramHelper.setcLast((String) rec.getVal("c_last").asJavaVal());
		paramHelper.setcCredit((String) rec.getVal("c_credit").asJavaVal());

		// INSERT INTO orders (o_id, o_w_id, o_d_id, o_c_id, o_entry_d,
		// o_carrier_id, o_ol_cnt, o_all_local) VALUES ( nextOId, wid,
		// did, cid, currentTime, 0, olCount, isAllLocal)
		paramHelper.setoEntryDate(System.currentTimeMillis());
		int isAllLocal = paramHelper.isAllLocal() ? 1 : 0;
		long oEntryDate = paramHelper.getoEntryDate();
		int olCount = paramHelper.getOlCount();

		fldVals = new HashMap<String, Constant>();
		fldVals.put("o_id", oidCon);
		fldVals.put("o_d_id", didCon);
		fldVals.put("o_w_id", widCon);
		fldVals.put("o_c_id", cidCon);
		fldVals.put("o_entry_d", new BigIntConstant(oEntryDate));
		fldVals.put("o_carrier_id", new IntegerConstant(0));
		fldVals.put("o_ol_cnt", new IntegerConstant(olCount));
		fldVals.put("o_all_local", new IntegerConstant(isAllLocal));
		insert(orderKey, fldVals);

		// INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES
		// (nextOId, did, wid)
		fldVals = new HashMap<String, Constant>();
		fldVals.put("no_o_id", oidCon);
		fldVals.put("no_d_id", didCon);
		fldVals.put("no_w_id", widCon);
		insert(newOrderKey, fldVals);

		// For each order line
		int totalAmount = 0;
		int[][] items = paramHelper.getItems();
		int orderLineCount = paramHelper.getOlCount();
		for (int i = 0; i < orderLineCount; i++) {

			// SELECT ... FROM item WHERE ...
			rec = readings.get(orderLineKeys[i][0]);
			double iPrice = (Double) rec.getVal("i_price").asJavaVal();

			// SELECT i_price, i_name, i_data FROM item WHERE i_id = olIId
			rec = readings.get(orderLineKeys[i][0]);
			rec.getVal("i_price").asJavaVal();
			rec.getVal("i_name").asJavaVal();
			rec.getVal("i_data").asJavaVal();

			// SELECT s_quantity, sDistXX, s_data, s_ytd, s_order_cnt FROM
			// stock WHERE s_i_id = olIId AND s_w_id = olSupplyWId
			String sDistXX;
			if (paramHelper.getDid() == 10)
				sDistXX = "s_dist_10";
			else
				sDistXX = "s_dist_0" + paramHelper.getDid();

			rec = readings.get(orderLineKeys[i][1]);
			rec.getVal("s_quantity").asJavaVal();
			rec.getVal(sDistXX).asJavaVal();
			rec.getVal("s_data").asJavaVal();
			rec.getVal("s_ytd").asJavaVal();
			rec.getVal("s_order_cnt").asJavaVal();

			// UPDATE stock SET s_quantity = ..., s_ytd = s_ytd + ol_quantitity,
			// s_order_cnt = s_order_cnt + 1 WHERE s_i_id = olIId AND
			// s_w_id = olSupplyWId
			rec = readings.get(orderLineKeys[i][1]);

			int olQuantity = items[i][2];
			int sQuantity = (Integer) rec.getVal("s_quantity").asJavaVal();
			int sYtd = (Integer) rec.getVal("s_ytd").asJavaVal();
			int sOrderCnt = (Integer) rec.getVal("s_order_cnt").asJavaVal();

			sQuantity -= olQuantity;
			if (sQuantity < 10)
				sQuantity += 91;
			sYtd += olQuantity;
			sOrderCnt++;

			Constant sDistInfoCon = rec.getVal(sDistXX);

			rec.setVal("s_quantity", new IntegerConstant(sQuantity));
			rec.setVal("s_ytd", new IntegerConstant(sYtd));
			rec.setVal("s_order_cnt", new IntegerConstant(sOrderCnt));

			update(orderLineKeys[i][1], rec);

			// INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id,
			// ol_number,ol_i_id, ol_supply_w_id, ol_delivery_d,
			// ol_quantity, ol_amount, ol_dist_info) VALUES (
			// nextOId, did, wid, i, olIId, olSupplyWid, NULL, olQuantity,
			// DoublePlainPrinter.toPlainString(olAmount), sDistInfo)
			int olIId = items[i][0];
			int supWid = items[i][1];
			double olAmount = olQuantity * iPrice;

			fldVals = new HashMap<String, Constant>();
			fldVals.put("ol_o_id", oidCon);
			fldVals.put("ol_d_id", didCon);
			fldVals.put("ol_w_id", widCon);
			fldVals.put("ol_number", new IntegerConstant(i + 1));
			fldVals.put("ol_i_id", new IntegerConstant(olIId));
			fldVals.put("ol_supply_w_id", new IntegerConstant(supWid));
			fldVals.put("ol_delivery_d", new BigIntConstant(Long.MIN_VALUE));
			fldVals.put("ol_quantity", new IntegerConstant(olQuantity));
			fldVals.put("ol_amount", new DoubleConstant(olAmount));
			fldVals.put("ol_dist_info", sDistInfoCon);
			insert(orderLineKeys[i][2], fldVals);

			// record amounts
			totalAmount += olAmount;
		}
		paramHelper.setTotalAmount(
				totalAmount * (1 - paramHelper.getcDiscount()) * (1 + paramHelper.getwTax() + paramHelper.getdTax()));

	}

}
