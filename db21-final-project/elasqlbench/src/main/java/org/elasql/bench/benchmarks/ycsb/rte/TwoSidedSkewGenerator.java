package org.elasql.bench.benchmarks.ycsb.rte;

import org.vanilladb.bench.benchmarks.tpcc.TpccValueGenerator;
import org.vanilladb.bench.benchmarks.ycsb.rte.YcsbZipfianGenerator;

public class TwoSidedSkewGenerator {

	private final YcsbZipfianGenerator leftZipfian;
	private final YcsbZipfianGenerator rightZipfian;
	TpccValueGenerator rvg = new TpccValueGenerator();
	
	private int recordCount;

	public TwoSidedSkewGenerator(int recordCount, double skewParameter) {
		this.recordCount = recordCount;
		leftZipfian = new YcsbZipfianGenerator(1, recordCount, skewParameter);
		rightZipfian = new YcsbZipfianGenerator(leftZipfian);
	}
	
	public TwoSidedSkewGenerator(TwoSidedSkewGenerator origin) {
		this.recordCount = origin.recordCount;
		leftZipfian = new YcsbZipfianGenerator(origin.leftZipfian);
		rightZipfian = new YcsbZipfianGenerator(origin.rightZipfian);
	}
	
	public long nextValue(long center) {
		long rawId, nextId;
		
		double leftSideProb = ((double) center) / recordCount;
		boolean isLeftSide = rvg.randomChooseFromDistribution(leftSideProb, 1 - leftSideProb) == 0;
		if (isLeftSide) {
			rawId = leftZipfian.nextLong(center);
			nextId = center - rawId + 1;
		} else {
			rawId = rightZipfian.nextLong(recordCount - center);
			nextId = center + rawId;
		}
		return nextId;
	}
	
	public static void main(String[] args) {
//		TwoSidedSkewGenerator gen = new TwoSidedSkewGenerator(1000, 0.99);
//		
//		for (int center = 100; center <= 1000; center += 100) {
//			System.out.print("Center at " + center + " : [");
//			for (int i = 0; i < 100; i++) {
//				System.out.print(gen.nextValue(center) + ", ");
//			}
//			System.out.println("]");
//		}
		
		TwoSidedSkewGenerator gen = new TwoSidedSkewGenerator(5, 0.9);
		
		for (int i = 0; i < 100; i++) {
			System.out.println(gen.nextValue(3));
		}
	}
}