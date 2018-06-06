package org.vanilladb.bench;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.bench.util.BenchProperties;

public class StatisticMgr {
	private static Logger logger = Logger.getLogger(StatisticMgr.class.getName());

	private static final String OUTPUT_DIR;

	private List<TxnResultSet> resultSets = new ArrayList<TxnResultSet>();

	private static long benchStartTime;

	private static int GRANULARITY;

	private static TreeMap<Long, ArrayList<Long>> latencyHistory;

	static {
		File defaultDir = new File(System.getProperty("user.home"), "benchmark_results");
		OUTPUT_DIR = BenchProperties.getLoader().getPropertyAsString(StatisticMgr.class.getName() + ".OUTPUT_DIR",
				defaultDir.getAbsolutePath());

		// Create the directory if that doesn't exist
		File dir = new File(OUTPUT_DIR);
		if (!dir.exists())
			dir.mkdir();
		benchStartTime = System.nanoTime();
		GRANULARITY = BenchProperties.getLoader().getPropertyAsInteger(StatisticMgr.class.getName() + ".GRANULARITY",
				3000);
		latencyHistory = new TreeMap<Long, ArrayList<Long>>();
	}

	private List<TransactionType> allTxTypes;

	public StatisticMgr(Collection<TransactionType> txTypes) {
		allTxTypes = new LinkedList<TransactionType>(txTypes);
	}

	public synchronized void processTxnResult(TxnResultSet trs) {
		resultSets.add(trs);
	}

	public synchronized void processBatchTxnsResult(TxnResultSet... trss) {
		for (TxnResultSet trs : trss)
			resultSets.add(trs);
	}

	public synchronized void outputReport() {
		HashMap<TransactionType, TxnStatistic> txnStatistics = new HashMap<TransactionType, TxnStatistic>();
		
		for (TransactionType type : allTxTypes)
			txnStatistics.put(type, new TxnStatistic(type));

		try {

			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
			String timeString = formatter.format(Calendar.getInstance().getTime());
			String fileName = timeString + "-details.txt";

			File dir = new File(OUTPUT_DIR);
			File outputFile = new File(dir, fileName);
			FileWriter wrFile = new FileWriter(outputFile);
			BufferedWriter bwrFile = new BufferedWriter(wrFile);
			
			int committedCount = 0;

			// write total transaction count
			bwrFile.write("# of txns (including aborts) during benchmark period: " + resultSets.size());
			bwrFile.newLine();

			// read all txn resultset
			bwrFile.write("Details of transactions: ");
			bwrFile.newLine();
			for (TxnResultSet resultSet : resultSets) {
				if (resultSet.isTxnIsCommited()) {
					bwrFile.write(resultSet.getTxnType() + ": "
							+ TimeUnit.NANOSECONDS.toMillis(resultSet.getTxnResponseTime()) + " ms");
					bwrFile.newLine();
					TxnStatistic txnStatistic = txnStatistics.get(resultSet.getTxnType());
					if (txnStatistic != null)
						txnStatistic.addTxnResponseTime(resultSet.getTxnResponseTime());
					addTxnLatency(resultSet);
					committedCount++;
				} else {
					bwrFile.write(resultSet.getTxnType() + ": aborted");
					bwrFile.newLine();
				}
			}
			bwrFile.newLine();

			// output summary for each transaction type
			for (Entry<TransactionType, TxnStatistic> entry : txnStatistics.entrySet()) {
				TxnStatistic value = entry.getValue();
				if (value.txnCount > 0) {
					long avgResTimeMs = TimeUnit.NANOSECONDS.toMillis(value.getTotalResponseTime() / value.txnCount);
					bwrFile.write(
							value.getmType() + " " + value.getTxnCount() + " avg latency: " + avgResTimeMs + " ms");
				} else {
					bwrFile.write(value.getmType() + " " + value.getTxnCount() + " avg latency: 0 ms");
				}
				bwrFile.newLine();
			}
			
			// TOTAL
			double avgResTimeMs = 0;
			
			if (committedCount > 0) {
				for (TxnResultSet rs : resultSets) {
					if (rs.isTxnIsCommited())
						avgResTimeMs += rs.getTxnResponseTime() / committedCount;
				}
			}

			bwrFile.write(String.format("Total %d Aborted %d Commited %d avg Commited latency: %d ms",
					resultSets.size(), resultSets.size() - committedCount, committedCount, Math.round(avgResTimeMs / 1000000)));

			bwrFile.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
			String timeString = formatter.format(Calendar.getInstance().getTime());
			String fileName = timeString + ".csv";

			File dir = new File(OUTPUT_DIR);
			File outputFile = new File(dir, fileName);
			FileWriter wrFile = new FileWriter(outputFile);
			BufferedWriter bwrFile = new BufferedWriter(wrFile);
			
			bwrFile.write("time(sec),throughput(txs),avg_latency(ms),min(ms),max(ms),25th_lat(ms),median_lat(ms),75th_lat(ms)\n");
			
			for (Map.Entry<Long, ArrayList<Long>> entry : latencyHistory.entrySet()) {
				List<Long> lats = entry.getValue();
				long stats[] = processLat(lats);
				long tp = lats.size() * 1000 / GRANULARITY;
				
				bwrFile.write(String.format("%d, %d, %d, %d, %d, %d, %d, %d\n",
						entry.getKey(), tp, stats[0], stats[1],
						stats[2], stats[3], stats[4], stats[5]));
			}

			bwrFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Finnish creating tpcc benchmark report");
	}

	public void addTxnLatency(TxnResultSet rs) {
		long t = TimeUnit.NANOSECONDS.toMillis(rs.getTxnEndTime() - benchStartTime);
		t = (((t - BenchmarkerParameters.WARM_UP_INTERVAL) / GRANULARITY) * GRANULARITY
				+ BenchmarkerParameters.WARM_UP_INTERVAL) / 1000;

		if (!latencyHistory.containsKey(t))
			latencyHistory.put(t, new ArrayList<Long>());

		latencyHistory.get(t).add(TimeUnit.NANOSECONDS.toMillis(rs.getTxnResponseTime()));
	}
	
	private long[] processLat(List<Long> lats) {
		long[] stats = new long[6];
		if (resultSets.size() == 0)
			return stats;
		Collections.sort(lats);
		
		stats[0] = findAvgLat(lats);
		stats[1] = findMinLat(lats);
		stats[2] = findMaxLat(lats);
		stats[3] = findFirstQuartileLat(lats);
		stats[4] = findSecondQuartileLat(lats);
		stats[5] = findThirdQuartileLat(lats);

		return stats;
	}
	
	private long findAvgLat(List<Long> sortedLats) {
		long sum = 0;
		for (long lat : sortedLats)
			sum += lat;
		return sum / sortedLats.size();
	}
	
	private long findMinLat(List<Long> sortedLats) {
		return sortedLats.get(0);
	}
	
	private long findMaxLat(List<Long> sortedLats) {
		return sortedLats.get(sortedLats.size() - 1);
	}
	
	private long findFirstQuartileLat(List<Long> sortedLats) {
		return sortedLats.get(sortedLats.size() / 4);
	}
	
	private long findSecondQuartileLat(List<Long> sortedLats) {
		return sortedLats.get(sortedLats.size() * 2 / 4);
	}
	
	private long findThirdQuartileLat(List<Long> sortedLats) {
		return sortedLats.get(sortedLats.size() * 3 / 4);
	}

	private static class TxnStatistic {
		private TransactionType mType;
		private int txnCount = 0;
		private long totalResponseTimeNs = 0;

		public TxnStatistic(TransactionType txnType) {
			this.mType = txnType;
		}

		public TransactionType getmType() {
			return mType;
		}

		public void addTxnResponseTime(long responseTime) {
			txnCount++;
			totalResponseTimeNs += responseTime;
		}

		public int getTxnCount() {
			return txnCount;
		}

		public long getTotalResponseTime() {
			return totalResponseTimeNs;
		}
	}
}