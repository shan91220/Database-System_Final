package org.vanilladb.bench.benchmarks.micro;

import org.vanilladb.bench.util.BenchProperties;

public class MicrobenchConstants {

	static {
		NUM_ITEMS = BenchProperties.getLoader().getPropertyAsInteger(
				MicrobenchConstants.class.getName() + ".NUM_ITEMS", 100000);
	}
	
	public static final int NUM_ITEMS;
	public static final int MIN_IM = 1;
	public static final int MAX_IM = 10000;
	public static final double MIN_PRICE = 1.00;
	public static final double MAX_PRICE = 100.00;
	public static final int MIN_I_NAME = 14;
	public static final int MAX_I_NAME = 24;
	public static final int MIN_I_DATA = 26;
	public static final int MAX_I_DATA = 50;
	public static final int MONEY_DECIMALS = 2;
}
