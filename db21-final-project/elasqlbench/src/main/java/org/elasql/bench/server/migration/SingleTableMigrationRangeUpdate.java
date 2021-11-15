package org.elasql.bench.server.migration;

import java.util.HashSet;
import java.util.Set;

import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;

public class SingleTableMigrationRangeUpdate implements MigrationRangeUpdate {
	
	private static final long serialVersionUID = 20181101001L;
	
	PartitioningKey partitioningKey;
	TableKeyIterator keyRangeToPush;
	int sourcePartId, destPartId;
	Set<PrimaryKey> otherMigratingKeys = new HashSet<PrimaryKey>();
	
	SingleTableMigrationRangeUpdate(int sourcePartId, int destPartId,
			PartitioningKey partitioningKey, TableKeyIterator keyRangeToPush, Set<PrimaryKey> otherMigratingKeys) {
		this.partitioningKey = partitioningKey;
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
