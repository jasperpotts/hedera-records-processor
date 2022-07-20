package com.swirlds.streamloader;

import com.swirlds.streamloader.data.PartProcessedRecordFile;
import com.swirlds.streamloader.data.ProcessedRecordFile;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.input.DiskRecordFileLoader;
import com.swirlds.streamloader.input.GCPRecordFileLoader;
import com.swirlds.streamloader.input.RecordFileLoader;
import com.swirlds.streamloader.output.FileOutputHandler;
import com.swirlds.streamloader.output.KafkaOutputHandler;
import com.swirlds.streamloader.output.OutputHandler;
import com.swirlds.streamloader.processing.SequentialRecordFileProcessor;
import com.swirlds.streamloader.processing.ParallelRecordFileProcessor;
import com.swirlds.streamloader.util.PrettyStatusPrinter;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class StreamDownloaderMain {
	// Create queue for future record files
	private static final ArrayBlockingQueue<Future<RecordFile>> recordFileQueue = new ArrayBlockingQueue<>(1000);
	// Create queue for future PartProcessedRecordFiles
	private static final ArrayBlockingQueue<Future<PartProcessedRecordFile>> partProcessedRecordFileQueue = new ArrayBlockingQueue<>(1000);
	// Create queue for ProcessedRecordFiles, these are fully processed and ready for output
	private static final ArrayBlockingQueue<ProcessedRecordFile> processedRecordFileQueue = new ArrayBlockingQueue<>(1000);

	public static void main(String[] args) throws Exception {
//		RecordFileLoader recordFileLoader = new DiskRecordFileLoader(Path.of("test-data/recordstreams"));
//		RecordFileLoader recordFileLoader = new DiskRecordFileLoader(Path.of("test-data/recordstreams/v2"));
//		RecordFileLoader recordFileLoader = new DiskRecordFileLoader(Path.of("test-data/recordstreams/mainnet-0.0.3"));
		RecordFileLoader recordFileLoader = new GCPRecordFileLoader(
				GCPRecordFileLoader.HederaNetwork.MAINNET,
				"0.0.3",
				Date.from(Instant.EPOCH)
		);
//		try (OutputHandler outputHandler = new FileOutputHandler()) {
		try (OutputHandler outputHandler = new KafkaOutputHandler("kafka")) {
			// start threads for parallel processing of record files
			doParallelProcessingOfRecordFiles(recordFileQueue, partProcessedRecordFileQueue);
			// start thread for sequential processing of record files
			doSequentialProcessingOfRecordFiles(partProcessedRecordFileQueue, processedRecordFileQueue);
			// start importing data into the pipeline we crated
			recordFileLoader.startLoadingRecordFiles(recordFileQueue);
			// start processing output in this thread, so we block till finished
			doOutput(processedRecordFileQueue, outputHandler);
		}
	}

	/**
	 * Creates a thread pool to process records from recordFileQueue and do all processing that can be done in parallel
	 * and out to partProcessedRecordFileQueue
	 */
	private static void doParallelProcessingOfRecordFiles(final ArrayBlockingQueue<Future<RecordFile>> recordFileQueue,
			final ArrayBlockingQueue<Future<PartProcessedRecordFile>> partProcessedRecordFileQueue) {
		final ThreadGroup downloadingExecutorsThreadGroup = new ThreadGroup("file-processors");
		final AtomicLong threadCount = new AtomicLong();
		final ExecutorService fileProcessingExecutorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
				runnable -> {
					Thread thread = new Thread(downloadingExecutorsThreadGroup, runnable, "file-processor-"+threadCount.incrementAndGet());
					thread.setDaemon(true);
					return thread;
				});
		// create thread to pull off first queue and schedule
		final Thread thread = new Thread(() -> {
			boolean lastFile = false;
			do {
				try {
					final RecordFile recordFile = recordFileQueue.take().get();
					lastFile = recordFile.isLastFile();
					partProcessedRecordFileQueue.put(
							fileProcessingExecutorService.submit(() -> ParallelRecordFileProcessor.processRecordFile(recordFile)));
				} catch (ExecutionException | InterruptedException e) {
					e.printStackTrace();
					System.err.flush();
					System.exit(1);
				}
			} while (!lastFile);
		}, "record file processing scheduler");
		// start threads
		thread.start();
	}

	private static void doSequentialProcessingOfRecordFiles(final ArrayBlockingQueue<Future<PartProcessedRecordFile>> partProcessedRecordFileQueue,
			final ArrayBlockingQueue<ProcessedRecordFile> processedRecordFileQueue) {
		// create thread to pull off first queue and schedule
		final Thread thread = new Thread(() -> {
			boolean lastFile = false;
			do {
				try {
					final PartProcessedRecordFile partProcessedRecordFile = partProcessedRecordFileQueue.take().get();
					if (partProcessedRecordFile != null) {
						lastFile = partProcessedRecordFile.isLastFile();
						final ProcessedRecordFile processedRecordFile = SequentialRecordFileProcessor.processBalances(
								partProcessedRecordFile);
						processedRecordFileQueue.put(processedRecordFile);
					}
				} catch (ExecutionException | InterruptedException e) {
					e.printStackTrace();
					System.err.flush();
					System.exit(1);
				}
			} while (!lastFile);
		}, "record file sequential processor");
		// start threads
		thread.start();
	}

	private static void doOutput(final ArrayBlockingQueue<ProcessedRecordFile> processedRecordFileQueue,
		OutputHandler outputHandler) {
		boolean lastFile = false;
		do {
			try {
				final ProcessedRecordFile processedRecordFile = processedRecordFileQueue.take();
				lastFile = processedRecordFile.isLastFile();
				outputHandler.outputRecordFile(processedRecordFile.recordFileRow());
				for (var transactionRowJson : processedRecordFile.transactionsRows()) {
					outputHandler.outputTransaction(transactionRowJson);
				}
				PrettyStatusPrinter.updateQueueSize("recordFileQueue",recordFileQueue.size());
				PrettyStatusPrinter.updateQueueSize("partProcessedRecordFileQueue",partProcessedRecordFileQueue.size());
				PrettyStatusPrinter.updateQueueSize("processedRecordFileQueue",processedRecordFileQueue.size());
				PrettyStatusPrinter.printStatusUpdate(processedRecordFile.startConsensusTimestamp(),
						processedRecordFile.transactionsRows().size());
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.err.flush();
				System.exit(1);
			}
		} while (!lastFile);
	}
}