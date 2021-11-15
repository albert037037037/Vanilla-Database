package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.recovery.CheckpointTask;
import org.vanilladb.core.storage.tx.recovery.RecoveryMgr;

public class YcsbTestbedLoaderProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(YcsbTestbedLoaderProc.class.getName());
	
	private int loadedCount = 0;
	
	public YcsbTestbedLoaderProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.newDefaultParamHelper());
	}

	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// do nothing
		// XXX: We should lock those tables
		// List<String> writeTables = Arrays.asList(paramHelper.getTables());
		// localWriteTables.addAll(writeTables);
	}
	
	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start loading testbed...");

		// turn off logging set value to speed up loading process
		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(false);
		
		// Generate records
		switch (ElasqlYcsbConstants.DATABASE_MODE) {
		case SINGLE_TABLE:
			populateSingleTableDB();
			break;
			
		case MULTI_TABLE:
			populateMultiTableDB();
			break;
			
		default:
			throw new RuntimeException("You should not be here");
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading completed. Flush all loading data to disks...");

		// TODO: remove this hack code in the future
		RecoveryMgr.enableLogging(true);

		// Create a checkpoint
		CheckpointTask cpt = new CheckpointTask();
		cpt.createCheckpoint();

		// Delete the log file and create a new one
		VanillaDb.logMgr().removeAndCreateNewLog();

		if (logger.isLoggable(Level.INFO))
			logger.info("Loading procedure finished. " + loadedCount + " YCSB records are loaded.");
	}
	
	private void populateSingleTableDB() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Populating single table YCSB database...");
		
		int numOfParts = Elasql.partitionMetaMgr().getCurrentNumOfParts();
		generateRecords("ycsb", 1, ElasqlYcsbConstants.INIT_RECORD_PER_PART * numOfParts);
	}
	
	private void populateMultiTableDB() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Populating multi-tenants YCSB database...");
		
		throw new UnsupportedOperationException("Unimplemented");
		
		// The below design is out-dated
//		int serverId = Elasql.serverId();
//		int tenantStartId = serverId * ElasqlYcsbConstants.TENANTS_PER_PART;
//		int tenantEndId = (serverId + 1) * ElasqlYcsbConstants.TENANTS_PER_PART;
//		int recordPerTenant = ElasqlYcsbConstants.INIT_RECORD_PER_PART / ElasqlYcsbConstants.TENANTS_PER_PART;
//		for (int tenantId = tenantStartId; tenantId < tenantEndId; tenantId++) {
//			String tableName = String.format("ycsb%d", tenantId);
//			generateRecords(tableName, 1, recordPerTenant);
//		}
	}
	
	private void generateRecords(String tableName, int startId, int recordCount) {
		int endId = startId + recordCount - 1;
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format(
					"Start populating table %s from id = %d to id = %d (count = %d)",
					tableName, startId, endId, recordCount));
		
		// Generate the field names of YCSB table
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("INSERT INTO %s (%s_id", tableName, tableName));
		for (int count = 1; count < YcsbConstants.FIELD_COUNT; count++) {
			sb.append(String.format(", %s_%d", tableName, count));
		}
		sb.append(") VALUES (");
		String sqlPrefix = sb.toString();
		
		// Generate records
		String idFieldName = tableName + "_id";
		String ycsbId, ycsbValue;
		PrimaryKey key;
		for (int id = startId; id <= endId; id++) {
			
			// The primary key of YCSB is the string format of id
			ycsbId = String.format(YcsbConstants.ID_FORMAT, id);
			
			// Check if it is a local record
			key = new PrimaryKey(tableName, idFieldName, new VarcharConstant(ycsbId));
			if (Elasql.partitionMetaMgr().getPartition(key) == Elasql.serverId()) {
				
				sb = new StringBuilder();
				sb.append(sqlPrefix);
				sb.append(String.format("'%s'", ycsbId));
				
				// Optimization: all fields use the same value
				ycsbValue = ycsbId;
				
				for (int count = 1; count < YcsbConstants.FIELD_COUNT; count++) {
					sb.append(String.format(", '%s'", ycsbValue));
				}
				sb.append(")");
	
				int result = VanillaDb.newPlanner().executeUpdate(sb.toString(),
						getTransaction());
				if (result <= 0)
					throw new RuntimeException();
				
				loadedCount++;
				if (loadedCount % 50000 == 0)
					if (logger.isLoggable(Level.INFO))
						logger.info(loadedCount + " YCSB records has been populated.");
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Populating YCSB table completed.");
	}
}
