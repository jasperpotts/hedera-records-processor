package com.swirlds.streamloader.processing;

import com.swirlds.streamloader.input.GoogleStorageFileLoader;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static com.swirlds.streamloader.util.Utils.getEpocNanosFromUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordFileDownloaderBlockTest {
	static {
		// make sure URL protocol is created
		new GoogleStorageFileLoader(GoogleStorageFileLoader.HederaNetwork.MAINNET,"0.0.3");
	}

	@Test
	void processDataItem() throws IOException, URISyntaxException {
		final PipelineLifecycle lifecycle = new PipelineLifecycle();
		final RecordFileDownloaderBlock block = new RecordFileDownloaderBlock(lifecycle);
		final URL url = new URL("gs://hedera-mainnet-streams/recordstreams/record0.0.3/2019-09-13T22_29_50.071933Z.rcd");
		final var recordFile = block.processDataItem(url);
		assertTrue(recordFile.startConsensusTime() > getEpocNanosFromUrl(url));
		assertTrue(recordFile.endConsensusTime() > getEpocNanosFromUrl(url));
		assertEquals(2, recordFile.fileVersion());
		assertEquals("0.3.0", recordFile.hapiVersion());
		assertEquals(5719, recordFile.sizeBytes());
		assertEquals(14, recordFile.transactions().length);
		assertEquals(14, recordFile.transactionRecords().length);
		System.out.println("recordFile = " + recordFile);
	}
}