package com.swirlds.streamloader.util;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pipeline building block for building chains of processing. Order of data in maintained, so each output is directly
 * corresponding to an input item.
 *
 * @param <I> type for input data
 * @param <O> type for output data
 */
public abstract class PipelineBlock<I,O> implements PipelineConsumer<I> {
	protected static final int QUEUE_SIZE = 1000;
	protected static final int THREAD_POOL_SIZE_FOR_PARALLEL = Math.min(16,Runtime.getRuntime().availableProcessors());
	protected final CopyOnWriteArrayList<PipelineConsumer<O>> outputConsumers = new CopyOnWriteArrayList<>();

	protected final String name;

	protected final PipelineLifecycle pipelineLifecycle;

	public PipelineBlock(final String name, final PipelineLifecycle pipelineLifecycle) {
		this.name = name;
		this.pipelineLifecycle = pipelineLifecycle;
		// add a new pipelineBlock
		pipelineLifecycle.addPipelineBlock(name);
	}

	// TODO the generics here are killing me, got to be better way
	@SuppressWarnings("unchecked")
	@SafeVarargs
	public final <P extends PipelineBlock<I,O>> P addOutputConsumer(final PipelineConsumer<O>... consumers) {
		outputConsumers.addAll(Arrays.asList(consumers));
		return (P)this;
	}

	protected void sendToConsumers(final O output, final boolean isLast) {
		for(var consumer : outputConsumers) {
			consumer.accept(output, isLast);
		}
	}

	public abstract O processDataItem(I input) throws Exception;

	public abstract int getQueueSize();

	/**
	 * Implementation of PipelineBlock that executes work in parallel in a thread pool.
	 */
	public static abstract class Parallel<I,O> extends PipelineBlock<I,O> {
		private final ArrayBlockingQueue<Future<O>> outputQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
		private final ThreadPoolExecutor threadPoolExecutor;
		/** Start count at one so that it is always above 0 and only when we get last item we decrement by two */
		protected final AtomicInteger count = new AtomicInteger(1);
		public Parallel(final String name, final PipelineLifecycle pipelineLifecycle) {
			this(name,pipelineLifecycle,2);
		}
		public Parallel(final String name, final PipelineLifecycle pipelineLifecycle, int numOfThreads) {
			super(name, pipelineLifecycle);
			// create a thread pool
			final ThreadGroup threadGroup = new ThreadGroup(name+"-processors");
			final AtomicLong threadCount = new AtomicLong();
			threadPoolExecutor = new ThreadPoolExecutor(
					numOfThreads,Runtime.getRuntime().availableProcessors()*2, 30, TimeUnit.SECONDS,
					new ArrayBlockingQueue<>(QUEUE_SIZE),
					runnable -> {
						Thread thread = new Thread(threadGroup, runnable, name+"-processor-"+threadCount.incrementAndGet());
						thread.setDaemon(true);
						return thread;
					});
			// create thread for reading from output queue
			Thread thread = new Thread(() -> {
				boolean isLast = false;
				do {
					try {
						final var future = outputQueue.take();
						final var countAfter = count.decrementAndGet();
						O result = null;
						try {
							result = future.get();
						} catch (Throwable e) {
							e.printStackTrace();
							Thread.sleep(50000);
						}
						isLast = countAfter == 0;
						sendToConsumers(result, isLast);
					} catch (InterruptedException e) {
						Utils.failWithError(e);
					}
				} while (!isLast);
				threadPoolExecutor.shutdown();
				pipelineLifecycle.pipelineBlockFinished(name);
			}, name+"-output-handler");
			thread.start();
		}

		@Override
		public void accept(final I i, final boolean isLast) {
			try {
				// don't add to count for last item as we want count to equal 0 and we started at 1
				if (!isLast) count.incrementAndGet();
				outputQueue.put(threadPoolExecutor.submit(() -> processDataItem(i)));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public int getQueueSize() {
			return outputQueue.size();
		}
	}

	public static abstract class Sequential<I,O> extends PipelineBlock<I,O> {
		protected final ArrayBlockingQueue<I> inputQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
		/** Start count at one so that it is always above 0 and only when we get last item we decrement by two */
		protected final AtomicInteger count = new AtomicInteger(1);
		public Sequential(final String name, final PipelineLifecycle pipelineLifecycle) {
			super(name, pipelineLifecycle);
			final Thread inputProcessingThread = new Thread(() -> {
				boolean isLast = false;
				do {
					try {
						final var item = inputQueue.take();
						final var countAfter = count.decrementAndGet();
						try {
							isLast = countAfter == 0;
							sendToConsumers(processDataItem(item), isLast);
						} catch (Exception e) {
							e.printStackTrace();
						}
					} catch (InterruptedException e) {
						Utils.failWithError(e);
					}
				} while(!isLast);
				pipelineLifecycle.pipelineBlockFinished(name);
			}, name);
			inputProcessingThread.start();
		}

		@Override
		public void accept(final I i, final boolean isLast) {
			try {
				// don't add to count for last item as we want count to equal 0 and we started at 1
				if (!isLast) count.incrementAndGet();
				this.inputQueue.put(i);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public int getQueueSize() {
			return inputQueue.size();
		}
	}
}

