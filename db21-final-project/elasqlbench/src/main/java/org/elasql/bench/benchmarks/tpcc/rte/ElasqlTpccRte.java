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
package org.elasql.bench.benchmarks.tpcc.rte;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.bench.benchmarks.tpcc.TpccTransactionType;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.bench.rte.RemoteTerminalEmulator;
import org.vanilladb.bench.rte.TransactionExecutor;

public class ElasqlTpccRte extends RemoteTerminalEmulator<TpccTransactionType> {
	private static Logger logger = Logger.getLogger(ElasqlTpccRte.class.getName());
	
	private int homeWid;
	private static Random txnTypeRandom;
	private Map<TpccTransactionType, ElasqlTpccTxExecutor> executors;

	public ElasqlTpccRte(SutConnection conn, StatisticMgr statMgr, int homeWarehouseId, int homeDistrictId) {
		super(conn, statMgr);
		
		if (logger.isLoggable(Level.FINE))
			logger.fine(String.format("TPCC RTE for warehouse %d, district %d is created.",
					homeWarehouseId, homeDistrictId));
		
		homeWid = homeWarehouseId;
		txnTypeRandom = new Random();
		executors = new HashMap<TpccTransactionType, ElasqlTpccTxExecutor>();
		executors.put(TpccTransactionType.NEW_ORDER, new ElasqlTpccTxExecutor(new NewOrderParamGen(homeWid, homeDistrictId)));
		executors.put(TpccTransactionType.PAYMENT, new ElasqlTpccTxExecutor(new PaymentParamGen(homeWid)));
		// TODO: Not implemented
//		executors.put(TpccTransactionType.ORDER_STATUS, new TpccTxExecutor(new OrderStatusParamGen(homeWid)));
//		executors.put(TpccTransactionType.DELIVERY, new TpccTxExecutor(new DeliveryParamGen(homeWid)));
//		executors.put(TpccTransactionType.STOCK_LEVEL, new TpccTxExecutor(new StockLevelParamGen(homeWid)));
	}
	
	protected TpccTransactionType getNextTxType() {
		int index = txnTypeRandom.nextInt(TpccConstants.FREQUENCY_TOTAL);
		if (index < TpccConstants.RANGE_NEW_ORDER)
			return TpccTransactionType.NEW_ORDER;
		else if (index < TpccConstants.RANGE_PAYMENT)
			return TpccTransactionType.PAYMENT;
		else if (index < TpccConstants.RANGE_ORDER_STATUS)
			return TpccTransactionType.ORDER_STATUS;
		else if (index < TpccConstants.RANGE_DELIVERY)
			return TpccTransactionType.DELIVERY;
		else
			return TpccTransactionType.STOCK_LEVEL;
	}
	
	protected TransactionExecutor<TpccTransactionType> getTxExeutor(TpccTransactionType type) {
		return executors.get(type);
	}
}
