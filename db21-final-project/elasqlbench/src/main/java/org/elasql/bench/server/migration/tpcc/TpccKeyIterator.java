package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class TpccKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	private ArrayList<String> tableNames = new ArrayList<String>();
	private Map<String, TableKeyIterator> tableIterators =
			new HashMap<String, TableKeyIterator>();
	private int currentTableIndex = 0;
	
	// TODO: write a test case
	public static void main(String[] args) {
		
		System.setProperty("org.elasql.storage.metadata.PartitionMetaMgr.NUM_PARTITIONS", "3");
		
		TpccKeyIterator keyIter = new TpccKeyIterator(11, 2);
		
		for (int i = 0; i < 200; i++) {
			System.out.println(keyIter.next());
		}

		PrimaryKeyBuilder builder = new PrimaryKeyBuilder("warehouse");
		builder.addFldVal("w_id", new IntegerConstant(11));
		PrimaryKey key = builder.build();
		System.out.println(keyIter.isInSubsequentKeys(key));

		builder.setVal("w_id", new IntegerConstant(12));
		key = builder.build();
		System.out.println(keyIter.isInSubsequentKeys(key));

		builder = new PrimaryKeyBuilder("customer");
		builder.addFldVal("c_w_id", new IntegerConstant(11));
		builder.addFldVal("c_d_id", new IntegerConstant(5));
		builder.addFldVal("c_id", new IntegerConstant(10));
		key = builder.build();
		System.out.println(keyIter.isInSubsequentKeys(key));

		builder = new PrimaryKeyBuilder("customer");
		builder.addFldVal("c_w_id", new IntegerConstant(12));
		builder.addFldVal("c_d_id", new IntegerConstant(5));
		builder.addFldVal("c_id", new IntegerConstant(10));
		key = builder.build();
		System.out.println(keyIter.isInSubsequentKeys(key));
		
		new TpccKeyIterator(keyIter);
	}
	
	public TpccKeyIterator(int startWid, int wcount) {
		addTableIterator(new WarehouseKeyIterator(startWid, wcount));
		addTableIterator(new DistrictKeyIterator(startWid, wcount));
		addTableIterator(new CustomerKeyIterator(startWid, wcount));
		addTableIterator(new HistoryKeyIterator(startWid, wcount));
		addTableIterator(new NewOrderKeyIterator(startWid, wcount));
		addTableIterator(new OrdersKeyIterator(startWid, wcount));
		addTableIterator(new OrderLineKeyIterator(startWid, wcount));
		addTableIterator(new StockKeyIterator(startWid, wcount));
	}
	
	public TpccKeyIterator(TpccKeyIterator iter) {
		addTableIterator(new WarehouseKeyIterator((WarehouseKeyIterator) iter.tableIterators.get("warehouse")));
		addTableIterator(new DistrictKeyIterator((DistrictKeyIterator) iter.tableIterators.get("district")));
		addTableIterator(new CustomerKeyIterator((CustomerKeyIterator) iter.tableIterators.get("customer")));
		addTableIterator(new HistoryKeyIterator((HistoryKeyIterator) iter.tableIterators.get("history")));
		addTableIterator(new NewOrderKeyIterator((NewOrderKeyIterator) iter.tableIterators.get("new_order")));
		addTableIterator(new OrdersKeyIterator((OrdersKeyIterator) iter.tableIterators.get("orders")));
		addTableIterator(new OrderLineKeyIterator((OrderLineKeyIterator) iter.tableIterators.get("order_line")));
		addTableIterator(new StockKeyIterator((StockKeyIterator) iter.tableIterators.get("stock")));
		currentTableIndex = iter.currentTableIndex;
	}

	@Override
	public boolean hasNext() {
		for (TableKeyIterator iter : tableIterators.values()) {
			if (iter.hasNext())
				return true;
		}
		return false;
	}

	@Override
	public PrimaryKey next() {
		String tableName = tableNames.get(currentTableIndex);
		TableKeyIterator iter = tableIterators.get(tableName);
		
		while (!iter.hasNext()) {
			currentTableIndex = (currentTableIndex + 1) % tableNames.size();
			tableName = tableNames.get(currentTableIndex);
			iter = tableIterators.get(tableName);
		}
		
		// move to the next
		currentTableIndex = (currentTableIndex + 1) % tableNames.size();
		
		return iter.next();
	}

	@Override
	public String getTableName() {
		return null;
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		TableKeyIterator iter = tableIterators.get(key.getTableName());
		return iter.isInSubsequentKeys(key);
	}
	
	@Override
	public TableKeyIterator copy() {
		return new TpccKeyIterator(this);
	}
	
	private void addTableIterator(TableKeyIterator iterator) {
		tableNames.add(iterator.getTableName());
		tableIterators.put(iterator.getTableName(), iterator);
	}
}
