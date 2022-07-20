package com.swirlds.streamloader.util;

import java.time.Instant;
import java.util.LinkedList;

import static com.swirlds.streamloader.util.Utils.getInstantFromNanoEpicLong;

public class PrettyStatusPrinter {
	private final LinkedList<Long> listOfPrevConsensusTimes = new LinkedList<>();
	private final LinkedList<Integer> speedsForRollingAverage = new LinkedList<>();
	private long lastResetTime = System.nanoTime();
	private long transactionsProcessed = 0;
	private long recordFilesProcessed = 0;
	private double transactionsProcessedASecond = 0;
	private double recordFilesProcessedASecond = 0;

	/**
	 * Called once per record file
	 */
	public void printStatusUpdate(long consensusTime, long numOfTransactionsProcessed) {
		recordFilesProcessed ++;
		transactionsProcessed += numOfTransactionsProcessed;
		listOfPrevConsensusTimes.add(consensusTime);
		if (listOfPrevConsensusTimes.size() >= 100) {
			final long now = System.nanoTime();
			final long realElapsedNanos = now - lastResetTime;
			lastResetTime = now;
			final long consensusElapsedNanos = listOfPrevConsensusTimes.getLast() - listOfPrevConsensusTimes.getFirst();
			speedsForRollingAverage.add((int)((double) consensusElapsedNanos / (double) realElapsedNanos));
			if (speedsForRollingAverage.size() >= 100) speedsForRollingAverage.removeFirst();
			listOfPrevConsensusTimes.clear();

			final double elapsedSeconds = (double)realElapsedNanos / 1_000_000_000.0;
			transactionsProcessedASecond = ( (double)numOfTransactionsProcessed/ elapsedSeconds);
			recordFilesProcessedASecond = ( (double)100/ elapsedSeconds);
		}
		@SuppressWarnings("OptionalGetWithoutIsPresent") final int averageSpeed = speedsForRollingAverage.isEmpty() ?
				0 : (int)speedsForRollingAverage.stream().mapToInt(Integer::intValue).average().getAsDouble();
		final Instant consensusInstant = getInstantFromNanoEpicLong(consensusTime);
		System.out.printf("\rProcessing Time = %S @ %,dx realtime -- Transactions %,d @ %,.1f/sec-- Files %,d @ %,.1f/sec",
				consensusInstant.toString(), averageSpeed, transactionsProcessed, transactionsProcessedASecond,
				recordFilesProcessed, recordFilesProcessedASecond);
	}
}
