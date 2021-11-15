package org.vanilladb.core.storage.file;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.vanilladb.core.server.ServerInit;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.buffer.BufferTest;
import org.vanilladb.core.storage.file.io.IoAllocator;
import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.util.BarrierStartRunner;

import junit.framework.Assert;

public class FileMgrConcurrencyTest {
	private static Logger logger = Logger.getLogger(FileMgrConcurrencyTest.class.getName());

	private static final String FILE_NAME_PREFIX = "_temp_filemgr_test_";
	private static final int OPENER_COUNT = 100;
	
	@BeforeClass
	public static void init() {
		ServerInit.init(BufferTest.class);
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BEGIN FILEMGR CONCURRENCY TEST");
	}
	
	@AfterClass
	public static void finish() {
		if (logger.isLoggable(Level.INFO))
			logger.info("FINISH FILEMGR CONCURRENCY TEST");
	}
	
	@Test
	public void testConcurrentOpenAndDelete() {
		FileMgr fileMgr = VanillaDb.fileMgr();
		
		// Create multiple threads for opening files
		CyclicBarrier startBarrier = new CyclicBarrier(OPENER_COUNT);
		CyclicBarrier endBarrier = new CyclicBarrier(OPENER_COUNT + 1);
		Opener[] openers = new Opener[OPENER_COUNT];
		for (int i = 0; i < OPENER_COUNT; i++) {
			openers[i] = new Opener(startBarrier, endBarrier, fileMgr, i);
			openers[i].start();
		}

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		// Check the results
		for (Opener opener : openers) {
			if (opener.getException() != null) {
				Assert.fail("Exception happens when writing file '" + opener.blk.fileName()
						+ "'\n" + opener.getException());
			}
			
		}
		
		// Create multiple threads for deleting and opening other files
		startBarrier = new CyclicBarrier(OPENER_COUNT);
		endBarrier = new CyclicBarrier(OPENER_COUNT + 1);
		openers = new Opener[OPENER_COUNT];
		Deleter[] deleters = new Deleter[OPENER_COUNT];
		for (int i = 0; i < OPENER_COUNT; i++) {
			openers[i] = new Opener(startBarrier, endBarrier, fileMgr, OPENER_COUNT + i);
			openers[i].start();
			deleters[i] = new Deleter(startBarrier, endBarrier, fileMgr, i);
			deleters[i].start();
		}

		// Wait for running
		try {
			endBarrier.await();
		} catch (InterruptedException | BrokenBarrierException e) {
			e.printStackTrace();
		}
		
		// Check the results
		for (Opener opener : openers) {
			if (opener.getException() != null) {
				Assert.fail("Exception happens when writing file '" + opener.blk.fileName()
						+ "'\n" + opener.getException());
			}
		}
		for (Deleter deleter : deleters) {
			if (deleter.getException() != null) {
				Assert.fail("Exception happens when removing file '" + deleter.fileName
						+ "'\n" + deleter.getException());
			}
		}
	}
	
	class Opener extends BarrierStartRunner {

		FileMgr fileMgr;
		BlockId blk;
		IoBuffer buffer;

		public Opener(CyclicBarrier startBarrier, CyclicBarrier endBarrier, FileMgr fileMgr,
				int id) {
			super(startBarrier, endBarrier);

			this.fileMgr = fileMgr;
			this.blk = new BlockId(FILE_NAME_PREFIX + id, 0);
			this.buffer = IoAllocator.newIoBuffer(Page.BLOCK_SIZE);
		}

		@Override
		public void runTask() {
			fileMgr.write(blk, buffer);
		}
	}
	
	class Deleter extends BarrierStartRunner {

		FileMgr fileMgr;
		String fileName;

		public Deleter(CyclicBarrier startBarrier, CyclicBarrier endBarrier, FileMgr fileMgr,
				int id) {
			super(startBarrier, endBarrier);

			this.fileMgr = fileMgr;
			this.fileName = FILE_NAME_PREFIX + id;
		}

		@Override
		public void runTask() {
			fileMgr.delete(fileName);
		}
	}
}
