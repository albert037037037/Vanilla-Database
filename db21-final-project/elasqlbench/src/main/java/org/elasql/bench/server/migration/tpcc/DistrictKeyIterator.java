package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class DistrictKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid, did;
	
	private boolean hasNext = true;
	private PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("district");
	
	public DistrictKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		
		initKeyBuilder();
	}
	
	public DistrictKeyIterator(DistrictKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
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
		keyBuilder.setVal("d_w_id", new IntegerConstant(wid));
		keyBuilder.setVal("d_id", new IntegerConstant(did));
		
		// move to the next
		did++;
		if (did > 10) {
			wid++;
			did = 1;
			
			if (wid > endWid) {
				hasNext = false;
			}
		}
		
		return keyBuilder.build();
	}

	@Override
	public String getTableName() {
		return "district";
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		if (!key.getTableName().equals("district"))
			return false;
		
		Integer keyWid = (Integer) key.getVal("d_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyDid = (Integer) key.getVal("d_id").asJavaVal();
			return keyDid >= did;
		}
	}
	
	@Override
	public TableKeyIterator copy() {
		return new DistrictKeyIterator(this);
	}
	
	private void initKeyBuilder() {
		keyBuilder.addFldVal("d_w_id", new IntegerConstant(wid));
		keyBuilder.addFldVal("d_id", new IntegerConstant(did));
	}

}
