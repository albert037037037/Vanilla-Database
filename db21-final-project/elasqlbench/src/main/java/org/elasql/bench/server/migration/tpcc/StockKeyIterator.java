package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class StockKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid, iid;
	
	private boolean hasNext = true;
	private PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("stock");
	
	public StockKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
		this.iid = 1;
		
		initKeyBuilder();
	}
	
	public StockKeyIterator(StockKeyIterator iter) {
		this.wid = iter.wid;
		this.iid = iter.iid;
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
		keyBuilder.setVal("s_i_id", new IntegerConstant(iid));
		keyBuilder.setVal("s_w_id", new IntegerConstant(wid));
		
		// move to the next
		iid++;
		if (iid > 100000) {
			wid++;
			iid = 1;
			
			if (wid > endWid) {
				hasNext = false;
			}
		}
		
		return keyBuilder.build();
	}

	@Override
	public String getTableName() {
		return "stock";
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		if (!key.getTableName().equals("stock"))
			return false;
		
		Integer keyWid = (Integer) key.getVal("s_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyIid = (Integer) key.getVal("s_i_id").asJavaVal();
			return keyIid >= iid;
		}
	}
	
	@Override
	public TableKeyIterator copy() {
		return new StockKeyIterator(this);
	}
	
	private void initKeyBuilder() {
		keyBuilder.addFldVal("s_i_id", new IntegerConstant(iid));
		keyBuilder.addFldVal("s_w_id", new IntegerConstant(wid));
	}

}
