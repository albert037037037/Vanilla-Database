package org.elasql.bench.server.migration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasql.sql.PrimaryKey;

public class MultiTableKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	private ArrayList<String> tableNames = new ArrayList<String>();
	private Map<String, TableKeyIterator> tableIterators =
			new HashMap<String, TableKeyIterator>();
	private int currentTableIndex = 0;
	
	public MultiTableKeyIterator() {
		
	}
	
	public MultiTableKeyIterator(MultiTableKeyIterator iter) {
		for (String tblName : iter.tableNames) {
			addTableIterator(iter.tableIterators.get(tblName).copy());
		}
		currentTableIndex = iter.currentTableIndex;
	}
	
	public void addTableIterator(TableKeyIterator iterator) {
		tableNames.add(iterator.getTableName());
		tableIterators.put(iterator.getTableName(), iterator);
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
		return new MultiTableKeyIterator(this);
	}
}
