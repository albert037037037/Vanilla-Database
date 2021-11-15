package org.elasql.bench.util;

import java.util.concurrent.locks.ReentrantLock;

import org.vanilladb.bench.BenchmarkerParameters;

public class NodeStatisticsRecorder extends Thread {

	private class NodeStatistics {
		long txCount = 0, latencySum = 0l;
		
		NodeStatistics add(long latency) {
			NodeStatistics newStat = new NodeStatistics();
			
			newStat.txCount = this.txCount + 1;
			newStat.latencySum = this.latencySum + latency;
			
			return newStat;
		}
	}
	
	private int nodeCount;
	private long startTime;
	private long totalTime;
	private long recordInterval;
	private NodeStatistics[] statistics;
	private ReentrantLock[] nodeLocks;
	
	public NodeStatisticsRecorder(int nodeCount, long startTime, long recordInterval) {
		this.nodeCount = nodeCount;
		this.startTime = startTime;
		this.recordInterval = recordInterval;
		
		this.totalTime = BenchmarkerParameters.WARM_UP_INTERVAL + BenchmarkerParameters.BENCHMARK_INTERVAL + recordInterval;
		this.statistics = new NodeStatistics[nodeCount];
		this.nodeLocks = new ReentrantLock[nodeCount];
		for (int i = 0; i < nodeCount; i++) {
			this.statistics[i] = new NodeStatistics();
			this.nodeLocks[i] = new ReentrantLock();
		}
	}
	
	@Override
	public void run() {
		long recordCount = 0;
		long elapsedTime = System.currentTimeMillis() - startTime;
		
		while (elapsedTime < totalTime) {
			// Record tx counts
			if (elapsedTime >= (recordCount + 1) * recordInterval) {
				// Retrieve the values
				recordCount++;
				NodeStatistics[] stats = takeASnapshotAndReset();
				
				// Calculate other statistics
				long totalTxCount = 0;
				long latencySum = 0;
				long[] avgNodeLatency = new long[nodeCount];
				
				for (int i = 0; i < nodeCount; i++) {
					totalTxCount += stats[i].txCount;
					latencySum += stats[i].latencySum;
					avgNodeLatency[i] = countAverage(stats[i].latencySum, stats[i].txCount);
				}

				long avgLatency = countAverage(latencySum, totalTxCount);
				
				// Print the results
				StringBuilder sb = new StringBuilder();
				
				sb.append("== Statistics at " + (elapsedTime / 1000) + " second ==\n");
				sb.append("- Total Throughput: " + totalTxCount + "\n");
				sb.append("- Each Node Throughput: " + stats[0].txCount);
				for (int i = 1; i < nodeCount; i++) {
					sb.append(", ");
					sb.append(stats[i].txCount);
				}
				sb.append('\n');
				
				sb.append("- Overall Average Latency (in us): " + avgLatency + "\n");
				sb.append("- Each Node Average Latency: " + avgNodeLatency[0]);
				for (int i = 1; i < nodeCount; i++) {
					sb.append(", ");
					sb.append(avgNodeLatency[i]);
				}
				sb.append('\n');
				
				// Print the result
				System.out.println(sb.toString());
			}
			
			// Sleep for a short time (avoid busy waiting)
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			// Update elapsed time
			elapsedTime = System.currentTimeMillis() - startTime;
		}
	}
	
	public void addTxResult(int node, long latency) {
		nodeLocks[node].lock();
		
		NodeStatistics newStat = statistics[node].add(latency);
		statistics[node] = newStat;
		
		nodeLocks[node].unlock();
	}
	
	private NodeStatistics[] takeASnapshotAndReset() {
		NodeStatistics[] stats = new NodeStatistics[nodeCount];
		
		for (int i = 0; i < nodeCount; i++) {
			nodeLocks[i].lock();
			stats[i] = statistics[i];
			statistics[i] = new NodeStatistics();
			nodeLocks[i].unlock();
		}
		
		return stats;
	}
	
	private long countAverage(long total, long count) {
		if (count > 0)
			return total / count;
		return 0;
	}
}
