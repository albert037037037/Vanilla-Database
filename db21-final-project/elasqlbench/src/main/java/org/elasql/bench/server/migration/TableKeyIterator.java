package org.elasql.bench.server.migration;

import java.util.Iterator;

import org.elasql.sql.PrimaryKey;

public interface TableKeyIterator extends Iterator<PrimaryKey> {
	
	String getTableName();
	
	boolean isInSubsequentKeys(PrimaryKey key);
	
	TableKeyIterator copy();
	
}
