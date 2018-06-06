package org.vanilladb.bench;

import org.vanilladb.bench.util.BenchProperties;

public class BenchmarkerParameters {
	
	public static final long WARM_UP_INTERVAL;
	public static final long BENCHMARK_INTERVAL;
	public static final int NUM_RTES;
	
	public static final String SERVER_IP;
	
	// JDBC = 1, SP = 2
	public static enum ConnectionMode { JDBC, SP };
	public static final ConnectionMode CONNECTION_MODE;

	static {
		WARM_UP_INTERVAL = BenchProperties.getLoader().getPropertyAsLong(
				BenchmarkerParameters.class.getName() + ".WARM_UP_INTERVAL", 60000);

		BENCHMARK_INTERVAL = BenchProperties.getLoader().getPropertyAsLong(
				BenchmarkerParameters.class.getName() + ".BENCHMARK_INTERVAL",
				60000);

		NUM_RTES = BenchProperties.getLoader().getPropertyAsInteger(
				BenchmarkerParameters.class.getName() + ".NUM_RTES", 1);
		
		SERVER_IP = BenchProperties.getLoader().getPropertyAsString(
				BenchmarkerParameters.class.getName() + ".SERVER_IP", "127.0.0.1");
		
		int conMode = BenchProperties.getLoader().getPropertyAsInteger(
				BenchmarkerParameters.class.getName() + ".CONNECTION_MODE", 1);
		switch (conMode) {
		case 1:
			CONNECTION_MODE = ConnectionMode.JDBC;
			break;
		case 2:
			CONNECTION_MODE = ConnectionMode.SP;
			break;
		default:
			throw new IllegalArgumentException("The connection mode should be 1 (JDBC) or 2 (SP)");
		}
	}
}