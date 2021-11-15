package org.elasql.bench.server.procedure.calvin.micro;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.bench.benchmarks.micro.ElasqlMicrobenchConstants;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecute2pcProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MicroCheckDatabaseProc extends AllExecute2pcProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(MicroCheckDatabaseProc.class.getName());
	
	public MicroCheckDatabaseProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.newDefaultParamHelper());
	}
	
	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Checking database for the micro benchmarks...");

		// Checking item records
		int startIId = Elasql.serverId() * ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE + 1;
		int endIId = (Elasql.serverId() + 1) * ElasqlMicrobenchConstants.NUM_ITEMS_PER_NODE;
		if (!checkItemTable(startIId, endIId))
			abort("checking database fails");

		if (logger.isLoggable(Level.INFO))
			logger.info("Checking completed.");
	}

	private boolean checkItemTable(int startIId, int endIId) {
		if (logger.isLoggable(Level.FINE))
			logger.fine("Checking items from i_id=" + startIId + " to i_id=" + endIId);
		
		// Use a bit array to record existence
		int total = endIId - startIId + 1;
		boolean[] checked = new boolean[total];
		for (int i = 0; i < total; i++)
			checked[i] = false;
		
		// Scan the table
		String sql = "SELECT i_id FROM item";
		Scan scan = StoredProcedureHelper.executeQuery(sql, getTransaction());
		scan.beforeFirst();
		for (int count = 0; count < total; count++) {
			if (!scan.next()) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe(String.format("Only %d records are found (there should be %d records)",
							count, total));
				return false;
			}
			
			int id = (Integer) scan.getVal("i_id").asJavaVal();
			if (checked[id - startIId]) {
				if (logger.isLoggable(Level.SEVERE))
					logger.severe(String.format("Found duplicated record (i_id = %d)", id));
				return false;
			}
			checked[id - startIId] = true;
			count++;
		}
		scan.close();

		if (logger.isLoggable(Level.FINE))
			logger.fine("Checking items completed.");
		
		return true;
	}

}
