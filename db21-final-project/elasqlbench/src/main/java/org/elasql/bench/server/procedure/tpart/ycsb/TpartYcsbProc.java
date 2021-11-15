package org.elasql.bench.server.procedure.tpart.ycsb;

import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.param.ycsb.ElasqlYcsbProcParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class TpartYcsbProc extends TPartStoredProcedure<ElasqlYcsbProcParamHelper> {

	private PrimaryKey[] readKeys;
	private PrimaryKey[] writeKeys;
	private PrimaryKey[] insertKeys;
	private Map<PrimaryKey, Constant> writeConstantMap = new HashMap<PrimaryKey, Constant>();
	private boolean doingReplication;
	
	public TpartYcsbProc(long txNum, boolean doingReplication) {
		super(txNum, new ElasqlYcsbProcParamHelper());
		this.doingReplication = doingReplication;
	}
	
	@Override
	protected void prepareKeys() {
		// set read keys
		readKeys = new PrimaryKey[paramHelper.getReadCount()];
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			// create RecordKey for reading
			PrimaryKey key = paramHelper.getReadKey(i);
			readKeys[i] = key;
			addReadKey(key);
		}
		
		// set write keys
		writeKeys = new PrimaryKey[paramHelper.getWriteCount()];
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			// create record key for writing
			PrimaryKey key = paramHelper.getWriteKey(i);
			writeKeys[i] = key;
			addWriteKey(key);
			
			// Create key-value pairs for writing
			Constant c = new VarcharConstant(paramHelper.getWriteValue(i));
			writeConstantMap.put(key, c);
		}
		
		// set insert keys
		insertKeys = new PrimaryKey[paramHelper.getInsertCount()];
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			// create record key for inserting
			PrimaryKey key = paramHelper.getInsertKey(i);
			insertKeys[i] = key;
			addInsertKey(key);
		}
	}
	
	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// SELECT ycsb_id, ycsb_1 FROM ycsb WHERE ycsb_id = ...
		for (int idx = 0; idx < paramHelper.getReadCount(); idx++) {
			String fieldName = paramHelper.getReadTableName(idx) + "_1";
			CachedRecord rec = readings.get(readKeys[idx]);
			paramHelper.setReadVal((String) rec.getVal(fieldName).asJavaVal(), idx);
		}

		// UPDATE ycsb SET ycsb_1 = ... WHERE ycsb_id = ...
		for (int idx = 0; idx < writeKeys.length; idx++) {
			String fieldName = paramHelper.getWriteTableName(idx) + "_1";
			CachedRecord rec = readings.get(writeKeys[idx]);
			rec.setVal(fieldName, writeConstantMap.get(writeKeys[idx]));
			update(writeKeys[idx], rec);
		}
		
		// INSERT INTO ycsb (ycsb_id, ycsb_1, ...) VALUES ("...", "...", ...)
		for (int idx = 0; idx < paramHelper.getInsertCount(); idx++) {
			insert(insertKeys[idx], paramHelper.getInsertVals(idx));
		}
	}
	
	@Override
	public double getWeight() {
		return paramHelper.getReadCount() + paramHelper.getWriteCount() + 
				paramHelper.getInsertCount();
	}
	
	public boolean isDoingReplication() {
		return doingReplication;
	}
}
