package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.bench.server.procedure.calvin.tpcc.PaymentProc;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class HistoryKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int startWid, endWid;
	private int wid, did, cid, hid;
	private int[][][] maxHistoryIds;
	
	private boolean hasNext = true;
	private PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("history");
	
	public HistoryKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.startWid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		this.cid = 1;
		this.hid = 1;
		maxHistoryIds = new int[wcount][10][3000];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				for (int ci = 0; ci < 3000; ci++)
					maxHistoryIds[wi][di][ci] = PaymentProc.getNextHistoryId(wi + startWid, di + 1, ci + 1) - 1;
		
		initKeyBuilder();
	}
	
	public HistoryKeyIterator(HistoryKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
		this.cid = iter.cid;
		this.hid = iter.hid;
		this.startWid = iter.startWid;
		this.endWid = iter.endWid;
		this.hasNext = iter.hasNext;

		int wcount = endWid - startWid + 1;
		maxHistoryIds = new int[wcount][10][3000];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				for (int ci = 0; ci < 3000; ci++)
					maxHistoryIds[wi][di][ci] = iter.maxHistoryIds[wi][di][ci];
		
		initKeyBuilder();
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public PrimaryKey next() {
		keyBuilder.setVal("h_id", new IntegerConstant(hid));
		keyBuilder.setVal("h_c_id", new IntegerConstant(cid));
		keyBuilder.setVal("h_c_d_id", new IntegerConstant(did));
		keyBuilder.setVal("h_c_w_id", new IntegerConstant(wid));
		
		// move to the next
		hid++;
		if (hid > maxHistoryIds[wid - startWid][did - 1][cid - 1]) {
			hid = 1;
			cid++;
			
			if (cid > 3000) {
				cid = 1;
				did++;
				
				if (did > 10) {
					did = 1;
					wid++;
					
					if (wid > endWid) {
						hasNext = false;
					}
				}
			}
		}
		
		return keyBuilder.build();
	}

	@Override
	public String getTableName() {
		return "history";
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		if (!key.getTableName().equals("history"))
			return false;
		
		Integer keyWid = (Integer) key.getVal("h_c_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyDid = (Integer) key.getVal("h_c_d_id").asJavaVal();
			if (keyDid > did)
				return true;
			else if (keyDid < did)
				return false;
			else {
				Integer keyCid = (Integer) key.getVal("h_c_id").asJavaVal();
				if (keyCid > cid)
					return true;
				else if (keyCid < cid)
					return false;
				else {
					Integer keyHid = (Integer) key.getVal("h_id").asJavaVal();
					return keyHid >= hid;
				}
			}
		}
	}
	
	@Override
	public TableKeyIterator copy() {
		return new HistoryKeyIterator(this);
	}
	
	private void initKeyBuilder() {
		keyBuilder.addFldVal("h_id", new IntegerConstant(hid));
		keyBuilder.addFldVal("h_c_id", new IntegerConstant(cid));
		keyBuilder.addFldVal("h_c_d_id", new IntegerConstant(did));
		keyBuilder.addFldVal("h_c_w_id", new IntegerConstant(wid));
	}

}
