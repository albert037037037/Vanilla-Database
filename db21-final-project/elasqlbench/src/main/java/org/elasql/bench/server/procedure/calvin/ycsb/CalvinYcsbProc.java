package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.HashMap;
import java.util.Map;

import org.elasql.bench.server.param.ycsb.ElasqlYcsbProcParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class CalvinYcsbProc extends CalvinStoredProcedure<ElasqlYcsbProcParamHelper> {
	
	private PrimaryKey[] readKeys;
	private PrimaryKey[] writeKeys;
	private PrimaryKey[] insertKeys;
	private Map<PrimaryKey, Constant> writeConstantMap = new HashMap<PrimaryKey, Constant>();

	public CalvinYcsbProc(long txNum) {
		super(txNum, new ElasqlYcsbProcParamHelper());
	}

	@Override
	public void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// set read keys
		readKeys = new PrimaryKey[paramHelper.getReadCount()];
		for (int i = 0; i < paramHelper.getReadCount(); i++) {
			// create RecordKey for reading
			PrimaryKey key = paramHelper.getReadKey(i);
			readKeys[i] = key;
			analyzer.addReadKey(key);
		}

		// set write keys
		writeKeys = new PrimaryKey[paramHelper.getWriteCount()];
		for (int i = 0; i < paramHelper.getWriteCount(); i++) {
			// create record key for writing
			PrimaryKey key = paramHelper.getWriteKey(i);
			writeKeys[i] = key;
			analyzer.addUpdateKey(key);

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
			analyzer.addInsertKey(key);
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// SELECT ycsb_id, ycsb_1 FROM ycsb WHERE ycsb_id = ...
		for (int i = 0; i < readKeys.length; i++) {
			String fieldName = paramHelper.getReadTableName(i) + "_1";
			CachedRecord rec = readings.get(readKeys[i]);
			paramHelper.setReadVal((String) rec.getVal(fieldName).asJavaVal(), i);
		}

		// UPDATE ycsb SET ycsb_1 = ... WHERE ycsb_id = ...
		for (int i = 0; i < writeKeys.length; i++) {
			String fieldName = paramHelper.getWriteTableName(i) + "_1";
			CachedRecord rec = readings.get(writeKeys[i]);
			rec.setVal(fieldName, writeConstantMap.get(writeKeys[i]));
			update(writeKeys[i], rec);
		}

		// INSERT INTO ycsb (ycsb_id, ycsb_1, ...) VALUES ("...", "...", ...)
		for (int i = 0; i < paramHelper.getInsertCount(); i++) {
			insert(insertKeys[i], paramHelper.getInsertVals(i));
		}
	}
}
