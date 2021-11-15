package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class CustomerKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int endWid;
	private int wid, did, cid;
	
	private boolean hasNext = true;
	private PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("customer");
	
	public CustomerKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		this.cid = 1;
		
		initKeyBuilder();
	}
	
	public CustomerKeyIterator(CustomerKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
		this.cid = iter.cid;
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
		keyBuilder.setVal("c_w_id", new IntegerConstant(wid));
		keyBuilder.setVal("c_d_id", new IntegerConstant(did));
		keyBuilder.setVal("c_id", new IntegerConstant(cid));
		
		// move to the next
		cid++;
		if (cid > 3000) {
			did++;
			cid = 1;
			
			if (did > 10) {
				wid++;
				did = 1;
				
				if (wid > endWid) {
					hasNext = false;
				}
			}
		}
		
		return keyBuilder.build();
	}

	@Override
	public String getTableName() {
		return "customer";
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		if (!key.getTableName().equals("customer"))
			return false;
		
		Integer keyWid = (Integer) key.getVal("c_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyDid = (Integer) key.getVal("c_d_id").asJavaVal();
			if (keyDid > did)
				return true;
			else if (keyDid < did)
				return false;
			else {
				Integer keyCid = (Integer) key.getVal("c_id").asJavaVal();
				return keyCid >= cid;
			}
		}
	}
	
	@Override
	public TableKeyIterator copy() {
		return new CustomerKeyIterator(this);
	}
	
	private void initKeyBuilder() {
		keyBuilder.addFldVal("c_w_id", new IntegerConstant(wid));
		keyBuilder.addFldVal("c_d_id", new IntegerConstant(did));
		keyBuilder.addFldVal("c_id", new IntegerConstant(cid));
	}

}
