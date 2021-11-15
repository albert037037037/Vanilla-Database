package org.elasql.bench.server.migration;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.server.Elasql;
import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;

public class SingleTableMigrationRange implements MigrationRange {
	
	// Partitioning key
	private PartitioningKey partKey;
	private int sourcePartId, destPartId;
	
	private TableKeyIterator keyRangeToPush;
	private TableKeyIterator chunkGenerator;
	
	// For new inserted keys
	private Set<PrimaryKey> unmigratedNewKeys = new HashSet<PrimaryKey>();
	private ConcurrentLinkedQueue<PrimaryKey> nextMigratingNewKeys =
			new ConcurrentLinkedQueue<PrimaryKey>();
	private Set<PrimaryKey> newKeysInRecentChunk = new HashSet<PrimaryKey>();
	private boolean ignoreInsertion;
	
	// We does not remove the contents until the entire migration finishes
	private Set<PrimaryKey> migratedKeys = new HashSet<PrimaryKey>();
	
	// Note: this can only be called from the scheduler
	public SingleTableMigrationRange(int sourcePartId, int destPartId, PartitioningKey partitioningKey,
			TableKeyIterator keyIterator, boolean ignoreInsertion) {
		this.partKey = partitioningKey;
		this.sourcePartId = sourcePartId;
		this.destPartId = destPartId;
		this.keyRangeToPush = keyIterator.copy();
		this.chunkGenerator = keyIterator.copy();
		this.ignoreInsertion = ignoreInsertion;
	}
	
	@Override
	public boolean addKey(PrimaryKey key) {
		if (!contains(key))
			return false;
		
		if (ignoreInsertion)
			return true;
		
		unmigratedNewKeys.add(key);
		nextMigratingNewKeys.add(key);
		return true;
	}

	@Override
	public boolean contains(PrimaryKey key) {
		PartitioningKey partKey = Elasql.partitionMetaMgr().getPartitioningKey(key);
		return partKey.equals(partKey);
	}
	
	public boolean isMigrated(PrimaryKey key) {
		if (!migratedKeys.contains(key)) {
			if (unmigratedNewKeys.contains(key))
				return false;
			
			return !keyRangeToPush.isInSubsequentKeys(key);
		}
		return true;
	}
	
	public void setMigrated(PrimaryKey key) {
		if (unmigratedNewKeys.remove(key))
			return;
		
		if (keyRangeToPush.isInSubsequentKeys(key))
			migratedKeys.add(key);
	}
	
	// This may be called by another thread on the destination node
	/**
	 * If 'useBytesForSize' is enabled, it will use the bytes to represent the chunk size. If not,
	 * it will use the number of records. 
	 */
	public Set<PrimaryKey> generateNextMigrationChunk(boolean useBytesForSize, int maxChunkSize) {
		Set<PrimaryKey> chunk = new HashSet<PrimaryKey>();
		int chunkSize = 0;
		
		// Migrate the new inserted keys
		while (!nextMigratingNewKeys.isEmpty() && chunkSize < maxChunkSize) {
			PrimaryKey key = nextMigratingNewKeys.poll();
			
			// It is Ok that we do not check if the new key is migrated
			// because if it is migrated, we will prevent it from inserting.
			
			if (useBytesForSize)
				chunkSize += recordSize(key.getTableName());
			else
				chunkSize++;
			
			chunk.add(key);
			newKeysInRecentChunk.add(key);
		}
		
		// Migrate the other existing keys
		while (chunkGenerator.hasNext() && chunkSize < maxChunkSize) {
			PrimaryKey key = chunkGenerator.next();
			
			if (useBytesForSize)
				chunkSize += recordSize(key.getTableName());
			else
				chunkSize++;
			
			chunk.add(key);
		}
		
		return chunk;
	}
	
	/**
	 * MigrationRangeUpdate is used by background pushes to update the migration
	 * range in a single action. If we did not use this manner, it would require
	 * to record which keys are migrated. This might create large memory overhead.
	 * 
	 * @return
	 */
	@Override
	public MigrationRangeUpdate generateStatusUpdate() {
		return new SingleTableMigrationRangeUpdate(sourcePartId, destPartId,
				partKey, chunkGenerator.copy(), newKeysInRecentChunk);
	}

	@Override
	public boolean updateMigrationStatus(MigrationRangeUpdate update) {
		SingleTableMigrationRangeUpdate su = (SingleTableMigrationRangeUpdate) update;
		if (su.partitioningKey.equals(partKey)) {
			keyRangeToPush = su.keyRangeToPush;
			for (PrimaryKey key : su.otherMigratingKeys)
				setMigrated(key);
			return true;
		} else
			return false;
	}

	@Override
	public int getSourcePartId() {
		return sourcePartId;
	}

	@Override
	public int getDestPartId() {
		return destPartId;
	}
	
	private int recordSize(String tableName){
		switch(tableName){
		case "warehouse":
			return 344;
		case "district":
			return 352;
		case "customer":
			return 2552;
		case "history":
			return 132;
		case "new_order":
			return 12;
		case "orders":
			return 36;
		case "order_line":
			return 140;
		case "item":
			return 320;
		case "stock":
			return 1184;
		default:
			throw new IllegalArgumentException("No such table for TPCC");
		}
	}
	
	@Override
	public String toString() {
		return String.format("[partitioning key: %s, from node %d to node %d]", 
				partKey, sourcePartId, destPartId);
	}
}
