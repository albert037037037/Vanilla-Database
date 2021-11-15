package org.elasql.bench.server.migration;

import java.io.Serializable;

import org.elasql.sql.PrimaryKey;

public class DummyKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	private String tableName;
	
	public DummyKeyIterator(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public boolean hasNext() {
		return false;
	}

	@Override
	public PrimaryKey next() {
		return null;
	}

	@Override
	public String getTableName() {
		return tableName;
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		return false;
	}
	
	@Override
	public TableKeyIterator copy() {
		return new DummyKeyIterator(tableName);
	}
}
