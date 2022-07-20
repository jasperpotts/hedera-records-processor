package com.swirlds.streamloader.util;

import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import static com.swirlds.streamloader.util.Utils.getInstantFromNanoEpicLong;

public class PrettyStatusPrinter {
	private static final LinkedList<Long> listOfPrevConsensusTimes = new LinkedList<>();
	private static final LinkedList<Integer> speedsForRollingAverage = new LinkedList<>();
	private static long lastResetTime = System.nanoTime();
	private static long transactionsProcessed = 0;
	private static long recordFilesProcessed = 0;
	private static long balanceUpdatesProcessed = 0;
	private static ConcurrentHashMap<String,Integer> latestQueueSizes = new ConcurrentHashMap<>();


	/**
	 * Called once per record file
	 */
	public static void printStatusUpdate(long consensusTime, long numOfTransactionsProcessed,
			long balanceUpdates) {
		recordFilesProcessed ++;
		transactionsProcessed += numOfTransactionsProcessed;
		balanceUpdatesProcessed += balanceUpdates;
		listOfPrevConsensusTimes.add(consensusTime);
		if (listOfPrevConsensusTimes.size() >= 500) {
			final long now = System.nanoTime();
			final long realElapsedNanos = now - lastResetTime;
			lastResetTime = now;
			final long consensusElapsedNanos = listOfPrevConsensusTimes.getLast() - listOfPrevConsensusTimes.getFirst();
			speedsForRollingAverage.add((int)((double) consensusElapsedNanos / (double) realElapsedNanos));
			if (speedsForRollingAverage.size() >= 100) speedsForRollingAverage.removeFirst();
			listOfPrevConsensusTimes.clear();

			final double elapsedSeconds = (double)realElapsedNanos / 1_000_000_000.0;
			double transactionsProcessedASecond = ((double) numOfTransactionsProcessed / elapsedSeconds);
			double recordFilesProcessedASecond = ((double) 100 / elapsedSeconds);

			@SuppressWarnings("OptionalGetWithoutIsPresent") final int averageSpeed = speedsForRollingAverage.isEmpty() ?
					0 : (int)speedsForRollingAverage.stream().mapToInt(Integer::intValue).average().getAsDouble();
			final Instant consensusInstant = getInstantFromNanoEpicLong(consensusTime);
			System.out.printf("\rProcessing Time = %30s @ %,7dx realtime -- Transactions %,10d @ %,4.1f/sec -- Files %,7d @ %,4.1f/sec -- BalanceUpdates %,7d %s",
					consensusInstant.toString(), averageSpeed, transactionsProcessed, transactionsProcessedASecond,
					recordFilesProcessed, recordFilesProcessedASecond, balanceUpdatesProcessed,
					latestQueueSizes.reduceEntries(1,
							entry -> entry.getKey() + "=" + entry.getValue(), (str1, str2) -> str1 + ", " + str2));
		}
	}

	public static void updateQueueSize(String name, int size) {
		latestQueueSizes.put(name,size);
	}
}
