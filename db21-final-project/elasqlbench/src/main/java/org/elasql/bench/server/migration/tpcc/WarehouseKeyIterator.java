package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class WarehouseKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid;
	
	private boolean hasNext = true;
	private PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("warehouse");
	
	public WarehouseKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
		
		initKeyBuilder();
	}
	
	public WarehouseKeyIterator(WarehouseKeyIterator iter) {
		this.wid = iter.wid;
		this.endWid = iter.endWid;
		this.hasNext = iter.hasNext;
		
		initKeyBuilder();
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public PrimaryKey next() {
		keyBuilder.setVal("w_id", new IntegerConstant(wid));
		
		// move to the next
		wid++;
		if (wid > endWid) {
			hasNext = false;
		}
		
		return keyBuilder.build();
	}

	@Override
	public String getTableName() {
		return "warehouse";
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		if (!key.getTableName().equals("warehouse"))
			return false;
		
		Integer keyWid = (Integer) key.getVal("w_id").asJavaVal();
		return keyWid >= wid && keyWid <= endWid;
	}
	
	@Override
	public TableKeyIterator copy() {
		return new WarehouseKeyIterator(this);
	}
	
	private void initKeyBuilder() {
		keyBuilder.addFldVal("w_id", new IntegerConstant(wid));
	}
}
