package com.swirlds.streamloader;

import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.input.FileLoader;
import com.swirlds.streamloader.input.GoogleStorageFileLoader;
import com.swirlds.streamloader.output.AvroToFileThenGoogleOutputHandler;
import com.swirlds.streamloader.output.OutputHandler;
import com.swirlds.streamloader.processing.BalanceProcessingBlock;
import com.swirlds.streamloader.processing.BlockProcessingBlock;
import com.swirlds.streamloader.processing.EntityProcessingBlock;
import com.swirlds.streamloader.processing.NftProcessingBlock;
import com.swirlds.streamloader.processing.RecordFileDownloaderBlock;
import com.swirlds.streamloader.processing.RecordFileProcessingBlock;
import com.swirlds.streamloader.processing.TopicMessageProcessingBlock;
import com.swirlds.streamloader.processing.TransactionProcessingBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import com.swirlds.streamloader.util.Utils;
import org.apache.avro.generic.GenericRecord;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.nio.file.Path;
import java.util.Map;

public class StreamDownloaderMain {
	public static final String TRANSACTIONS_TOPIC = "transaction";
	public static final String RECORDS_TOPIC = "record";
	public static final String BALANCES_TOPIC = "balance";
	public static final String NFTS_TOPIC = "nft";
	public static final String TOPIC_MESSAGES_TOPIC = "topic-messages";

	public static final Map<String, Long> MAX_NUM_ROW_PER_FILE_MAP = Map.of(
			TRANSACTIONS_TOPIC, 250_000L,
			RECORDS_TOPIC, 30 * 60 * 24 * 15L, // 15 days
			BALANCES_TOPIC, 2_500_000L,
			NFTS_TOPIC, 500_000L,
			TOPIC_MESSAGES_TOPIC, 250_000L
	);

	static {
		Thread.setDefaultUncaughtExceptionHandler((t, e) -> Utils.failWithError(e));
	}

	public static void main(String[] args) throws Exception {
		FileLoader recordFileLoader = new GoogleStorageFileLoader(
				GoogleStorageFileLoader.HederaNetwork.MAINNET,
				"0.0.3"
		);
//		try (OutputHandler<GenericRecord> outputHandler = new AvroFileOutputHandler()) {
//		try (OutputHandler<GenericRecord> outputHandler = new AvroGoogleBucketFileOutputHandler("pinot-ingestion")) {
		try (OutputHandler<GenericRecord> outputHandler = new AvroToFileThenGoogleOutputHandler(Path.of("bucket-output"),"pinot-ingestion")) {
//		try (OutputHandler outputHandler = new KafkaOutputHandler("kafka")) {
			processRecords(recordFileLoader, outputHandler);
		}
	}

	public static void processRecords(FileLoader recordFileLoader, OutputHandler<GenericRecord> outputHandler) {
		// start with loading initial balances
		final ObjectLongHashMap<BalanceKey> balances = recordFileLoader.loadInitialBalances();
		// build processing pipeline
		final PipelineLifecycle lifecycle = new PipelineLifecycle();
		final var recordFileDownloaderBlock = new RecordFileDownloaderBlock(lifecycle)
				.addOutputConsumer(
						new BlockProcessingBlock(lifecycle)
								.addOutputConsumer(
										new EntityProcessingBlock(lifecycle).addOutputConsumer(outputHandler),
										new TransactionProcessingBlock(lifecycle).addOutputConsumer(outputHandler),
										new BalanceProcessingBlock(balances,lifecycle).addOutputConsumer(outputHandler),
										new RecordFileProcessingBlock(lifecycle).addOutputConsumer(outputHandler),
										new NftProcessingBlock(lifecycle).addOutputConsumer(outputHandler),
										new TopicMessageProcessingBlock(lifecycle).addOutputConsumer(outputHandler)
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
