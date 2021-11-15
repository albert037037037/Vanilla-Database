package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecute2pcProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class YcsbCheckDatabaseProc extends AllExecute2pcProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(YcsbTestbedLoaderProc.class.getName());
	
	public YcsbCheckDatabaseProc(long txNum) {
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
			logger.info("Checking database for the YCSB benchmarks...");
		
		// Generate records
		switch (ElasqlYcsbConstants.DATABASE_MODE) {
		case SINGLE_TABLE:
			checkSingleTableDB();
			break;
			
		case MULTI_TABLE:
			checkMultiTenantsDB();
			break;
			
		default:
			throw new RuntimeException("You should not be here");
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Checking completed.");
	}
	
	private void checkSingleTableDB() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Checking single table YCSB database...");
		
		int numOfParts = Elasql.partitionMetaMgr().getCurrentNumOfParts();
		if (!checkYcsbTable("ycsb", 1, ElasqlYcsbConstants.INIT_RECORD_PER_PART * numOfParts))
			abort("checking database fails");
	}
	
	private void checkMultiTenantsDB() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Checking multi-tenants YCSB database...");
		
		int serverId = Elasql.serverId();
		int tenantStartId = serverId * ElasqlYcsbConstants.TENANTS_PER_PART;
		int tenantEndId = (serverId + 1) * ElasqlYcsbConstants.TENANTS_PER_PART;
		int recordPerTenant = ElasqlYcsbConstants.INIT_RECORD_PER_PART / ElasqlYcsbConstants.TENANTS_PER_PART;
		for (int tenantId = tenantStartId; tenantId < tenantEndId; tenantId++) {
			String tableName = String.format("ycsb%d", tenantId);
			if (!checkYcsbTable(tableName, 1, recordPerTenant))
				abort("checking database fails");
		}
	}
	
	private boolean checkYcsbTable(String tableName, int startId, int recordCount) {
		int endId = startId + recordCount - 1;
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format(
					"Checking table %s from id = %d to id = %d (count = %d)",
					tableName, startId, endId, recordCount));
		
		// Use a bit array to record existence
		boolean[] checked = new boolean[recordCount];
		for (int i = 0; i < recordCount; i++)
			checked[i] = false;
		
		// Scan the table
		String sql = String.format("SELECT %s_id FROM %s", tableName, tableName);
		String fieldName = tableName + "_id";
		Scan scan = StoredProcedureHelper.executeQuery(sql, getTransaction());
		scan.beforeFirst();
		for (int i = startId, count = 0; i <= endId; i++) {
			if (!scan.next()) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe(String.format("Only %d records are found (there should be %d records)",
							count, recordCount));
				return false;
			}
			
			String idStr = (String) scan.getVal(fieldName).asJavaVal();
			int id = Integer.parseInt(idStr);
			if (checked[id - 1]) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe(String.format("Found duplicated record (id = %d)", id));
				return false;
			}
			checked[id - 1] = true;
			count++;
		}
		scan.close();

		if (logger.isLoggable(Level.FINE))
			logger.fine(String.format("Checking table %s completed.", tableName));
		
		return true;
	}
}
