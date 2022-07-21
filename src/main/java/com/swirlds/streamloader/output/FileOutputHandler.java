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
	private final static int MAX_OBJECTS_PER_FILE = 50_000;
	private final Path dataDir = Path.of("build/OUTPUT_DATA");
	private int transactionCount = 0;
	private int recordCount = 0;
	private int balanceCount = 0;
	private int recordFileNumber = 0;
	private int transactionFileNumber = 0;
	private int balanceFileNumber = 0;
	private BufferedWriter transactionWriter;
	private BufferedWriter recordWriter;
	private BufferedWriter balanceWriter;

	public FileOutputHandler() {
		try {
			if (Files.exists(dataDir)) {
				try (var pathStream = Files.list(dataDir)) {
					pathStream.forEach(path -> {
						try {
							Files.delete(path);
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
					});
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				Files.createDirectories(dataDir);
			}
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

	private void newBalanceFile() {
		balanceCount = 0;
		try {
			if (balanceWriter != null) {
				balanceWriter.flush();
				balanceWriter.close();
			}
			balanceWriter = Files.newBufferedWriter(
					dataDir.resolve("balances_"+(++balanceFileNumber)+".json"));
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
	public void outputAccountBalance(final JsonObject balanceJson) {
		try {
			if (balanceWriter == null) newBalanceFile();
			balanceWriter.write(balanceJson.toString()+"\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if ((++balanceCount) >= MAX_OBJECTS_PER_FILE) newRecordFile();
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
