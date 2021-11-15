package org.elasql.bench.server.migration.tpcc;

import org.elasql.bench.server.migration.DummyKeyIterator;
import org.elasql.bench.server.migration.SingleTableMigrationRange;
import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.sql.PartitioningKey;

public class TpccMigrationComponentFactory extends MigrationComponentFactory {

	@Override
	public MigrationPlan newPredefinedMigrationPlan() {
//		return new TpccPredefinedMigrationPlan();
		// TODO
		throw new UnsupportedOperationException("Unimplemented");
	}
	
	public MigrationRange toMigrationRange(int sourceId, int destId, PartitioningKey partitioningKey) {
		TableKeyIterator keyIterator = null;
		int wid;
		boolean ignoreInsertion = false;
		
		switch (partitioningKey.getTableName()) {
		case "warehouse":
			wid = (Integer) partitioningKey.getVal("w_id").asJavaVal();
			keyIterator = new WarehouseKeyIterator(wid, 1);
			break;
		case "district":
			wid = (Integer) partitioningKey.getVal("d_w_id").asJavaVal();
			keyIterator = new DistrictKeyIterator(wid, 1);
			break;
		case "stock":
			wid = (Integer) partitioningKey.getVal("s_w_id").asJavaVal();
			keyIterator = new StockKeyIterator(wid, 1);
			break;
		case "customer":
			wid = (Integer) partitioningKey.getVal("c_w_id").asJavaVal();
			keyIterator = new CustomerKeyIterator(wid, 1);
			break;
		case "history":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("h_c_w_id").asJavaVal();
//			keyIterator = new HistoryKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("history");
			ignoreInsertion = true;
			break;
		case "orders":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("o_w_id").asJavaVal();
//			keyIterator = new OrdersKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("orders");
			ignoreInsertion = true;
			break;
		case "new_order":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("no_w_id").asJavaVal();
//			keyIterator = new NewOrderKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("new_order");
			ignoreInsertion = true;
			break;
		case "order_line":
			// XXX: Skip this insert-only table to make migration faster
//			wid = (Integer) partitioningKey.getVal("ol_w_id").asJavaVal();
//			keyIterator = new OrderLineKeyIterator(wid, 1);
			keyIterator = new DummyKeyIterator("order_line");
			ignoreInsertion = true;
			break;
		default:
			return null;
		}
		
		return new SingleTableMigrationRange(sourceId, destId, partitioningKey, keyIterator, ignoreInsertion);
	}
}
