package com.swirlds.streamloader.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PipelineBlockTest {
	private static final int COUNT = 100;

	@Test
	void sequentialBlockTest() {
		PipelineLifecycle pipelineLifecycle = new PipelineLifecycle();
		PipelineBlock<Integer,Integer> block = new PlusMillionSequential("+1M", pipelineLifecycle);
		assertEquals(1_000_123, assertDoesNotThrow(() -> block.processDataItem(123)));
		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicInteger previous = new AtomicInteger(1_000_000);
		block.addOutputConsumer((data, isLast) -> {
			System.out.println("	Output data = " + data+" -- isLast = " + isLast);
			counter.incrementAndGet();
			assertEquals(previous.getAndIncrement(), data);
		});
		assertEquals(0,counter.get());
		for (int i = 0; i < COUNT; i++) {
			block.accept(i, i == (COUNT-1));
		}
		assertDoesNotThrow(pipelineLifecycle::waitForPipelineToFinish);
		assertEquals(COUNT,counter.get());
	}

	@Test
	void parallelBlockTest() {
		PipelineLifecycle pipelineLifecycle = new PipelineLifecycle();
		PipelineBlock<Integer,Integer> block = new PlusMillionParallel("+1M", pipelineLifecycle);
		assertEquals(1_000_123, assertDoesNotThrow(() -> block.processDataItem(123)));
		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicInteger previous = new AtomicInteger(1_000_000);
		block.addOutputConsumer((data, isLast) -> {
			System.out.println("	Output data = " + data+" -- isLast = " + isLast);
			counter.incrementAndGet();
			assertEquals(previous.getAndIncrement(), data);
		});
		assertEquals(0,counter.get());
		for (int i = 0; i < COUNT; i++) {
			block.accept(i, i == (COUNT-1));
		}
		assertDoesNotThrow(pipelineLifecycle::waitForPipelineToFinish);
		assertEquals(COUNT,counter.get());
	}

	@Test
	void chainTestTwoParallel() {
		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicInteger previous = new AtomicInteger(2_000_000);
		PipelineLifecycle pipelineLifecycle = new PipelineLifecycle();

		var block = new PlusMillionParallel("+1M #1",pipelineLifecycle)
				.addOutputConsumer(
						new PlusMillionParallel("+1M #2", pipelineLifecycle)
								.addOutputConsumer((data, isLast) -> {
									System.out.println("	Output data = " + data+" -- isLast = " + isLast);
									counter.incrementAndGet();
									assertEquals(previous.getAndIncrement(), data);
								}));

		assertEquals("+1M #1", block.name);
		assertEquals(0,counter.get());
		for (int i = 0; i < COUNT; i++) {
			block.accept(i, i == (COUNT-1));
		}
		assertDoesNotThrow(pipelineLifecycle::waitForPipelineToFinish);
		assertEquals(COUNT,counter.get());
	}

	@Test
	void chainTestTwoSequential() {
		final int baselineThreads = Thread.activeCount();
		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicInteger previous = new AtomicInteger(2_000_000);
		PipelineLifecycle pipelineLifecycle = new PipelineLifecycle();

		var block = new PlusMillionSequential("+1M #1",pipelineLifecycle)
				.addOutputConsumer(
						new PlusMillionSequential("+1M #2", pipelineLifecycle)
								.addOutputConsumer((data, isLast) -> {
									System.out.println("	Output data = " + data+" -- isLast = " + isLast);
									counter.incrementAndGet();
									assertEquals(previous.getAndIncrement(), data);
								}));

		assertEquals("+1M #1", block.name);
		assertEquals(2,Thread.activeCount()-baselineThreads);
		assertEquals(0,counter.get());
		for (int i = 0; i < COUNT; i++) {
			block.accept(i, i == (COUNT-1));
		}
		assertDoesNotThrow(pipelineLifecycle::waitForPipelineToFinish);
		assertEquals(COUNT,counter.get());
		assertEquals(0,Thread.activeCount()-baselineThreads);
	}
	@Test
	void chainTestMixed() {
		final int baselineThreads = Thread.activeCount();
		final AtomicInteger counter = new AtomicInteger(0);
		final AtomicInteger previous = new AtomicInteger(4_000_000);
		PipelineLifecycle pipelineLifecycle = new PipelineLifecycle();

		var block = new PlusMillionSequential("+1M #1",pipelineLifecycle)
				.addOutputConsumer(
						new PlusMillionParallel("+1M #2", pipelineLifecycle)
							.addOutputConsumer(
									new PlusMillionParallel("+1M #3", pipelineLifecycle)
										.addOutputConsumer(
												new PlusMillionSequential("+1M #4", pipelineLifecycle)
													.addOutputConsumer((data, isLast) -> {
														System.out.println("	Output data = " + data+" -- isLast = " + isLast);
														counter.incrementAndGet();
														assertEquals(previous.getAndIncrement(), data);
													}))
				));

		assertEquals("+1M #1", block.name);
		assertEquals(4,Thread.activeCount()-baselineThreads);
		assertEquals(0,counter.get());
		for (int i = 0; i < COUNT; i++) {
			block.accept(i, i == (COUNT-1));
		}
		assertDoesNotThrow(pipelineLifecycle::waitForPipelineToFinish);
		assertEquals(COUNT,counter.get());
		assertEquals(0,Thread.activeCount()-baselineThreads);
	}

	private static class PlusMillionParallel extends PipelineBlock.Parallel<Integer,Integer> {
		public PlusMillionParallel(final String name, final PipelineLifecycle pipelineLifecycle) {
			super(name, pipelineLifecycle);
		}

		@Override
		public Integer processDataItem(final Integer input) {
			System.out.println("Processed input = " + input +" to "+(input+1_000_000));
			return input+1_000_000;
		}
	}

	private static class PlusMillionSequential extends PipelineBlock.Sequential<Integer,Integer> {
		public PlusMillionSequential(final String name, final PipelineLifecycle pipelineLifecycle) {
			super(name, pipelineLifecycle);
		}

		@Override
		public Integer processDataItem(final Integer input) {
			System.out.println("Processed input = " + input +" to "+(input+1_000_000));
			return input+1_000_000;
		}
	}
}