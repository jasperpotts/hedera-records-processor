package com.swirlds.streamloader;

import com.swirlds.streamloader.input.DiskFileLoader;
import com.swirlds.streamloader.input.FileLoader;
import com.swirlds.streamloader.input.GoogleStorageFileLoader;
import com.swirlds.streamloader.output.AvroFileOutputHandler;
import com.swirlds.streamloader.output.JsonFileOutputHandler;
import com.swirlds.streamloader.output.OutputHandler;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

class StreamDownloaderMainTest {

	@Test
	public void testWithAllRecordsFiles() throws Exception {
		FileLoader recordFileLoader = new DiskFileLoader(Path.of("test-data/recordstreams"));
		try (OutputHandler outputHandler = new AvroFileOutputHandler()) {
			StreamDownloaderMain.processRecords(recordFileLoader, outputHandler);
		}
	}
	@Test
	public void testWithV2RecordsFiles() throws Exception {
		FileLoader recordFileLoader = new DiskFileLoader(Path.of("test-data/recordstreams/v2"));
		try (OutputHandler outputHandler = new AvroFileOutputHandler()) {
			StreamDownloaderMain.processRecords(recordFileLoader, outputHandler);
		}
	}
	@Test
	public void testWithV5RecordsFiles() throws Exception {
		FileLoader recordFileLoader = new DiskFileLoader(Path.of("test-data/recordstreams/v5"));
		try (OutputHandler outputHandler = new AvroFileOutputHandler()) {
			StreamDownloaderMain.processRecords(recordFileLoader, outputHandler);
		}
	}
	@Test
	public void testWithMainNetRecordsFiles() throws Exception {
		FileLoader recordFileLoader = new DiskFileLoader(Path.of("test-data/recordstreams/mainnet-0.0.3"));
		try (OutputHandler outputHandler = new AvroFileOutputHandler()) {
			StreamDownloaderMain.processRecords(recordFileLoader, outputHandler);
		}
	}
	@Test
	public void testWithGoogleMainNetFiles() throws Exception {
		FileLoader recordFileLoader = new GoogleStorageFileLoader(
				GoogleStorageFileLoader.HederaNetwork.MAINNET,
				"0.0.3"
		);
		try (OutputHandler outputHandler = new AvroFileOutputHandler()) {
			StreamDownloaderMain.processRecords(recordFileLoader, outputHandler);
		}
	}
}