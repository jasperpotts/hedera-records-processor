package com.swirlds.streamloader.util;

import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import static com.swirlds.streamloader.util.Utils.getInstantFromNanoEpicLong;

public class PrettyStatusPrinter {
	private final LinkedList<Long> listOfPrevConsensusTimes = new LinkedList<>();
	private final LinkedList<Integer> speedsForRollingAverage = new LinkedList<>();
	private long lastResetTime = System.nanoTime();

	public void printStatusUpdate(long consensusTime) {
		listOfPrevConsensusTimes.add(consensusTime);
		if (listOfPrevConsensusTimes.size() >= 100) {
			final long now = System.nanoTime();
			final long realElapsedNanos = now - lastResetTime;
			lastResetTime = now;
			final long consensusElapsedNanos = listOfPrevConsensusTimes.getLast() - listOfPrevConsensusTimes.getFirst();
			speedsForRollingAverage.add((int)((double) consensusElapsedNanos / (double) realElapsedNanos));
			if (speedsForRollingAverage.size() >= 100) speedsForRollingAverage.removeFirst();
			listOfPrevConsensusTimes.clear();
		}
		@SuppressWarnings("OptionalGetWithoutIsPresent") final int averageSpeed =
				speedsForRollingAverage.isEmpty() ? 0 : (int)speedsForRollingAverage.stream().mapToInt(Integer::intValue).average().getAsDouble();
		final Instant consensusInstant = getInstantFromNanoEpicLong(consensusTime);
		System.out.printf("\rProcessing Time = %S @ %,dx realtime", consensusInstant.toString(), averageSpeed);
	}
}
