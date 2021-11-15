package org.elasql.bench.server.procedure.calvin.ycsb;

import java.util.Map;

import org.elasql.bench.server.param.ycsb.ElasqlYcsbSchemaBuilderParamHelper;
import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.server.VanillaDb;

public class YcsbSchemaBuilderProc extends AllExecuteProcedure<ElasqlYcsbSchemaBuilderParamHelper> {

	public YcsbSchemaBuilderProc(long txNum) {
		super(txNum, new ElasqlYcsbSchemaBuilderParamHelper());
	}

	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		// Do nothing
		// XXX: We should lock those tables
		// localWriteTables.addAll(Arrays.asList(paramHelper.getTableNames()));
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		for (String cmd : paramHelper.getTableSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, getTransaction());
		for (String cmd : paramHelper.getIndexSchemas())
			VanillaDb.newPlanner().executeUpdate(cmd, getTransaction());
	}
}
