package com.swirlds.streamloader.output;

import javax.json.JsonObject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * OutputHandler that write json files for testing
 */
public class FileOutputHandler implements OutputHandler {
	private final static int MAX_OBJECTS_PER_FILE = 1000;
	private final Path dataDir = Path.of("build/OUTPUT_DATA");
	private int transactionCount = 0;
	private int recordCount = 0;
	private int recordFileNumber = 0;
	private int transactionFileNumber = 0;
	private BufferedWriter transactionWriter;
	private BufferedWriter recordWriter;

	public FileOutputHandler() {
		try {
			Files.createDirectories(dataDir);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void newTransactionFile() {
		transactionCount = 0;
		try {
			if (transactionWriter != null) {
				transactionWriter.flush();
				transactionWriter.close();
			}
			transactionWriter = Files.newBufferedWriter(
					dataDir.resolve("transactions_"+(++transactionFileNumber)+".json"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void newRecordFile() {
		recordCount = 0;
		try {
			if (recordWriter != null) {
				recordWriter.flush();
				recordWriter.close();
			}
			recordWriter = Files.newBufferedWriter(
					dataDir.resolve("records_"+(++recordFileNumber)+".json"));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void outputTransaction(final JsonObject transactionJson) {
		try {
			if (transactionWriter == null) newTransactionFile();
			transactionWriter.write(transactionJson.toString()+"\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if ((++transactionCount) >= MAX_OBJECTS_PER_FILE) newTransactionFile();
	}

	@Override
	public void outputRecordFile(final JsonObject recordFileJson) {
		try {
			if (recordWriter == null) newRecordFile();
			recordWriter.write(recordFileJson.toString()+"\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if ((++recordCount) >= MAX_OBJECTS_PER_FILE) newRecordFile();
	}

	@Override
	public void close() throws Exception {
		if (transactionWriter != null) {
			transactionWriter.flush();
			transactionWriter.close();
		}
		if (recordWriter != null) {
			recordWriter.flush();
			recordWriter.close();
		}
	}
}
