package com.swirlds.streamloader;

import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.input.FileLoader;
import com.swirlds.streamloader.input.GoogleStorageFileLoader;
import com.swirlds.streamloader.output.FileOutputHandler;
import com.swirlds.streamloader.output.KafkaOutputHandler;
import com.swirlds.streamloader.output.OutputHandler;
import com.swirlds.streamloader.processing.BalanceProcessingBlock;
import com.swirlds.streamloader.processing.BlockProcessingBlock;
import com.swirlds.streamloader.processing.RecordFileDownloaderBlock;
import com.swirlds.streamloader.processing.RecordFileProcessingBlock;
import com.swirlds.streamloader.processing.TransactionProcessingBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import com.swirlds.streamloader.util.Utils;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

public class StreamDownloaderMain {
	public static final String TRANSACTIONS_TOPIC = "transaction_record_new";
	public static final String RECORDS_TOPIC = "record_file_new";
	public static final String BALANCES_TOPIC = "balance";

	static {
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> Utils.failWithError(e));
	}

	public static void main(String[] args) throws Exception {
		FileLoader recordFileLoader = new GoogleStorageFileLoader(
				GoogleStorageFileLoader.HederaNetwork.MAINNET,
				"0.0.3"
		);
//		try (OutputHandler outputHandler = new FileOutputHandler()) {
		try (OutputHandler outputHandler = new KafkaOutputHandler("kafka")) {
			processRecords(recordFileLoader, outputHandler);
		}
	}

	public static void processRecords(FileLoader recordFileLoader, OutputHandler outputHandler) {
		// start with loading initial balances
		final ObjectLongHashMap<BalanceKey> balances = recordFileLoader.loadInitialBalances();
		// build processing pipeline
		final PipelineLifecycle lifecycle = new PipelineLifecycle();
		final var recordFileDownloaderBlock = new RecordFileDownloaderBlock(lifecycle)
				.addOutputConsumer(
						new BlockProcessingBlock(lifecycle)
								.addOutputConsumer(
										new TransactionProcessingBlock(lifecycle).addOutputConsumer(outputHandler),
										new BalanceProcessingBlock(balances,lifecycle).addOutputConsumer(outputHandler),
										new RecordFileProcessingBlock(lifecycle).addOutputConsumer(outputHandler)
								)
				);

		// start importing data into the pipeline we crated
		recordFileLoader.startLoadingRecordFileUrls(recordFileDownloaderBlock);
		// wait for pipeline to finish
		try {
			lifecycle.waitForPipelineToFinish();
		} catch (Exception e) {
			Utils.failWithError(e);
		}
	}
}