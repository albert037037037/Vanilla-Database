package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.bench.server.procedure.calvin.tpcc.NewOrderProc;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.bench.benchmarks.tpcc.TpccConstants;
import org.vanilladb.core.sql.IntegerConstant;

public class NewOrderKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int startWid, endWid;
	private int wid, did, oid;
	private int[][] maxOrderIds;
	
	private boolean hasNext = true;
	private PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("new_order");
	
	public NewOrderKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.startWid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		this.oid = TpccConstants.NEW_ORDER_START_ID;
		maxOrderIds = new int[wcount][10];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				maxOrderIds[wi][di] = NewOrderProc.getNextOrderId(wi + startWid, di + 1) - 1;
		
		initKeyBuilder();
	}
	
	public NewOrderKeyIterator(NewOrderKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
		this.oid = iter.oid;
		this.startWid = iter.startWid;
		this.endWid = iter.endWid;
		this.hasNext = iter.hasNext;

		int wcount = endWid - startWid + 1;
		maxOrderIds = new int[wcount][10];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				maxOrderIds[wi][di] = iter.maxOrderIds[wi][di];
		
		initKeyBuilder();
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public PrimaryKey next() {
		keyBuilder.setVal("no_w_id", new IntegerConstant(wid));
		keyBuilder.setVal("no_d_id", new IntegerConstant(did));
		keyBuilder.setVal("no_o_id", new IntegerConstant(oid));
		
		// move to the next
		oid++;
		if (oid > maxOrderIds[wid - startWid][did - 1]) {
			did++;
			oid = TpccConstants.NEW_ORDER_START_ID;
			
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
		return "new_order";
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		if (!key.getTableName().equals("new_order"))
			return false;
		
		Integer keyWid = (Integer) key.getVal("no_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyDid = (Integer) key.getVal("no_d_id").asJavaVal();
			if (keyDid > did)
				return true;
			else if (keyDid < did)
				return false;
			else {
				Integer keyOid = (Integer) key.getVal("no_o_id").asJavaVal();
				return keyOid >= oid;
			}
		}
	}
	
	@Override
	public TableKeyIterator copy() {
		return new NewOrderKeyIterator(this);
	}
	
	private void initKeyBuilder() {
		keyBuilder.addFldVal("no_w_id", new IntegerConstant(wid));
		keyBuilder.addFldVal("no_d_id", new IntegerConstant(did));
		keyBuilder.addFldVal("no_o_id", new IntegerConstant(oid));
	}

}
