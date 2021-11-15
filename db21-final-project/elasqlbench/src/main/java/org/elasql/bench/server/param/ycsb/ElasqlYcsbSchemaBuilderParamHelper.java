package org.elasql.bench.server.param.ycsb;

import org.elasql.bench.benchmarks.ycsb.ElasqlYcsbConstants;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.bench.benchmarks.ycsb.YcsbConstants;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class ElasqlYcsbSchemaBuilderParamHelper extends StoredProcedureParamHelper {
	
	private static final String[] TABLES_NAMES;
	private static final String[] TABLES_DDL;
	private static final String[] INDEXES_DDL;
	
	static {
		switch (ElasqlYcsbConstants.DATABASE_MODE) {
		case SINGLE_TABLE:
			// Table names
			TABLES_NAMES = new String[] { "ycsb" };
			
			// Table DDL
			TABLES_DDL = new String[] { genTableSql("ycsb") };
			
			// Index DDL
			INDEXES_DDL = new String[] { genIndexSql("ycsb") };
			
			break;
			
		case MULTI_TABLE:
			int numOfTenants = ElasqlYcsbConstants.TENANTS_PER_PART *
					PartitionMetaMgr.NUM_PARTITIONS;
			
			// Table names
			TABLES_NAMES = new String[numOfTenants];
			for (int i = 0; i < TABLES_NAMES.length; i++)
				TABLES_NAMES[i] = String.format("ycsb%d", i);
			
			// Table DDL
			TABLES_DDL = new String[numOfTenants];
			for (int i = 0; i < TABLES_DDL.length; i++)
				TABLES_DDL[i] = genTableSql(TABLES_NAMES[i]);
			
			// Index DDL
			INDEXES_DDL = new String[numOfTenants];
			for (int i = 0; i < INDEXES_DDL.length; i++)
				INDEXES_DDL[i] = genIndexSql(TABLES_NAMES[i]);
			
			break;
			
		default:
			throw new RuntimeException("You should not be here");
		}
	}
	
	private static String genTableSql(String tableName) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(String.format("CREATE TABLE %s ( %s_id VARCHAR(%d)",
				tableName, tableName, YcsbConstants.CHARS_PER_FIELD));
		for (int count = 1; count < YcsbConstants.FIELD_COUNT; count++) {
			sb.append(String.format(", %s_%d VARCHAR(%d)",
					tableName, count, YcsbConstants.CHARS_PER_FIELD));
		}
		sb.append(")");
		
		return sb.toString();
	}
	
	private static String genIndexSql(String tableName) {
		return String.format("CREATE INDEX idx_%s ON %s (%s_id)",
				tableName, tableName, tableName);
	}

	public String[] getTableSchemas() {
		return TABLES_DDL;
	}

	public String[] getIndexSchemas() {
		return INDEXES_DDL;
	}
	
	public String[] getTableNames() {
		return TABLES_NAMES;
	}

	@Override
	public void prepareParameters(Object... pars) {
		// nothing to do
	}

	@Override
	public Schema getResultSetSchema() {
		return new Schema();
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		return new SpResultRecord();
	}
}
