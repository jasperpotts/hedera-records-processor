package com.swirlds.streamloader.output;

import com.swirlds.streamloader.data.JsonRow;
import com.swirlds.streamloader.util.Utils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OutputHandler that write json files for testing
 */
public class JsonFileOutputHandler implements OutputHandler<JsonRow> {
	private final static int MAX_OBJECTS_PER_FILE = 10_000;
	private final Path dataDir = Path.of("build/OUTPUT_DATA");
	public final Map<String, FileSet> fileSets = new HashMap<>();

	public JsonFileOutputHandler() {
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

	@Override
	public void accept(final List<JsonRow> jsonRows) {
		for(var jsonRow: jsonRows) {
			try {
				final String topicTableName = jsonRow.topicTableName();
				FileSet fileSet = fileSets.get(topicTableName);
				if (fileSet == null) {
					fileSet = new FileSet();
					fileSets.put(topicTableName, fileSet);
					fileSet.writer = Files.newBufferedWriter(
							dataDir.resolve(topicTableName + "_" + (++fileSet.fileNumber) + ".json"));
				}
				fileSet.writer.write(jsonRow.json() + "\n");
				if (fileSet.count >= MAX_OBJECTS_PER_FILE) {
					fileSet.writer.flush();
					fileSet.writer.close();
					fileSet.writer = Files.newBufferedWriter(
							dataDir.resolve(topicTableName + "_" + (++fileSet.fileNumber) + ".json"));
				}
			} catch (IOException e) {
				Utils.failWithError(e);
			}
		}
	}

	@Override
	public void close() throws Exception {
		System.out.println("FileOutputHandler.close");
		for(var fileSet: fileSets.values()) {
			fileSet.writer.flush();
			fileSet.writer.close();
		}
	}

	private static class FileSet {
		BufferedWriter writer;
		int count = 0;
		int fileNumber = 0;
	}
}
