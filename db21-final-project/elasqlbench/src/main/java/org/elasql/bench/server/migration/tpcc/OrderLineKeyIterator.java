package org.elasql.bench.server.migration.tpcc;

import java.io.Serializable;

import org.elasql.bench.server.migration.TableKeyIterator;
import org.elasql.bench.server.procedure.calvin.tpcc.NewOrderProc;
import org.elasql.sql.PrimaryKey;
import org.elasql.sql.PrimaryKeyBuilder;
import org.vanilladb.core.sql.IntegerConstant;

public class OrderLineKeyIterator implements TableKeyIterator, Serializable {
	
	private static final long serialVersionUID = 20181107001L;
	
	// start and end are inclusive
	private int startWid, endWid;
	private int wid, did, oid, olnum;
	private int[][] maxOrderIds;
	
	private boolean hasNext = true;
	private PrimaryKeyBuilder keyBuilder = new PrimaryKeyBuilder("order_line");
	
	public OrderLineKeyIterator(int startWid, int wcount) {
		this.wid = startWid;
		this.startWid = startWid;
		this.endWid = startWid + wcount - 1;
		this.did = 1;
		this.oid = 1;
		this.olnum = 1;
		maxOrderIds = new int[wcount][10];
		for (int wi = 0; wi < wcount; wi++)
			for (int di = 0; di < 10; di++)
				maxOrderIds[wi][di] = NewOrderProc.getNextOrderId(wi + startWid, di + 1) - 1;
		
		initKeyBuilder();
	}
	
	public OrderLineKeyIterator(OrderLineKeyIterator iter) {
		this.wid = iter.wid;
		this.did = iter.did;
		this.oid = iter.oid;
		this.olnum = iter.olnum;
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
		keyBuilder.setVal("ol_o_id", new IntegerConstant(oid));
		keyBuilder.setVal("ol_d_id", new IntegerConstant(did));
		keyBuilder.setVal("ol_w_id", new IntegerConstant(wid));
		keyBuilder.setVal("ol_number", new IntegerConstant(olnum));
		
		// move to the next
		olnum++;
		
		if (olnum > 10) {
			oid++;
			olnum = 1;
		
			if (oid > maxOrderIds[wid - startWid][did - 1]) {
				did++;
				oid = 1;
				
				if (did > 10) {
					wid++;
					did = 1;
					
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
		return "order_line";
	}

	@Override
	public boolean isInSubsequentKeys(PrimaryKey key) {
		if (!key.getTableName().equals("order_line"))
			return false;
		
		Integer keyWid = (Integer) key.getVal("ol_w_id").asJavaVal();
		if (keyWid > wid && keyWid <= endWid)
			return true;
		else if (keyWid < wid)
			return false;
		else {
			Integer keyDid = (Integer) key.getVal("ol_d_id").asJavaVal();
			if (keyDid > did)
				return true;
			else if (keyDid < did)
				return false;
			else {
				Integer keyOid = (Integer) key.getVal("ol_o_id").asJavaVal();
				if (keyOid > oid)
					return true;
				else if (keyOid < oid)
					return false;
				else {
					Integer keyOlnum = (Integer) key.getVal("ol_number").asJavaVal();
					return keyOlnum >= olnum;
				}
			}
		}
	}
	
	@Override
	public TableKeyIterator copy() {
		return new OrderLineKeyIterator(this);
	}
	
	private void initKeyBuilder() {
		keyBuilder.addFldVal("ol_o_id", new IntegerConstant(oid));
		keyBuilder.addFldVal("ol_d_id", new IntegerConstant(did));
		keyBuilder.addFldVal("ol_w_id", new IntegerConstant(wid));
		keyBuilder.addFldVal("ol_number", new IntegerConstant(olnum));
	}

}
