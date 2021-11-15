package org.elasql.bench.server.migration.tpcc;

import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.PrimaryKey;

public class TpccMigrationRangeUpdate implements MigrationRangeUpdate {
	
	private static final long serialVersionUID = 20181101001L;
	
	int minWid;
	TpccKeyIterator keyRangeToPush;
	int sourcePartId, destPartId;
	Set<PrimaryKey> otherMigratingKeys = new HashSet<PrimaryKey>();
	
	TpccMigrationRangeUpdate(int sourcePartId, int destPartId,
			int minWid, TpccKeyIterator keyRangeToPush, Set<PrimaryKey> otherMigratingKeys) {
		this.minWid = minWid;
		this.keyRangeToPush = keyRangeToPush;
		this.otherMigratingKeys = otherMigratingKeys;
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
	}
	
	@Override
	public int getSourcePartId() {
		return sourcePartId;
	}

	@Override
	public int getDestPartId() {
		return destPartId;
	}
}
