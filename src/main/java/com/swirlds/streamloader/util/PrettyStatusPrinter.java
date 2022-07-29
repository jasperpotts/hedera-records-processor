package com.swirlds.streamloader.util;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;

import static com.swirlds.streamloader.util.Utils.getInstantFromNanoEpicLong;

public class PrettyStatusPrinter {
	private static final LinkedList<Long> listOfPrevConsensusTimes = new LinkedList<>();
	private static final LinkedList<Integer> speedsForRollingAverage = new LinkedList<>();
	private static long lastResetTime = System.nanoTime();
	private static long recordFilesProcessed = 0;


	/**
	 * Called once per record file
	 */
	public static void printStatusUpdate(long consensusTime) {
		recordFilesProcessed ++;
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
			double recordFilesProcessedASecond = ((double) 100 / elapsedSeconds);

			@SuppressWarnings("OptionalGetWithoutIsPresent") final int averageSpeed = speedsForRollingAverage.isEmpty() ?
					0 : (int)speedsForRollingAverage.stream().mapToInt(Integer::intValue).average().getAsDouble();
			final Instant consensusInstant = getInstantFromNanoEpicLong(consensusTime);
			final Duration consensusTimeTillNow = Duration.between(getInstantFromNanoEpicLong(consensusElapsedNanos), Instant.now());
			final Duration eta = consensusTimeTillNow.dividedBy(averageSpeed);

			System.out.printf("\rProcessing Time = %30s @ %,7dx realtime, ETA %4d:%02dh  -- Files %,7d @ %,4.1f/sec",
					consensusInstant.toString(), averageSpeed,
					eta.toHours(), eta.toMinutesPart(),
					recordFilesProcessed, recordFilesProcessedASecond);
		}
	}
}
