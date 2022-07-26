package com.swirlds.streamloader.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class PipelineLifecycle {

	/** Countdown latch to allow threads to wait for end of all pipelines */
	private final CountDownLatch finishedLatch = new CountDownLatch(1);
	private final AtomicInteger pipelineBlocksCount = new AtomicInteger(0);

	protected void addPipelineBlock(String name) {
		final int count = pipelineBlocksCount.incrementAndGet();
		System.out.println("PipelineLifecycle.addPipelineBlock name="+name+" count="+count);
	}

	protected void pipelineBlockFinished(String name) {
		final int count = pipelineBlocksCount.decrementAndGet();
		System.out.println("PipelineLifecycle.pipelineBlockFinished name="+name+" count="+count);
		if (count == 0) {
			finishedLatch.countDown();
		}
	}

	public void waitForPipelineToFinish() throws InterruptedException {
		finishedLatch.await();
	}

}
