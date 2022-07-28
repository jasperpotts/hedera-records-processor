package com.swirlds.streamloader.output;

import com.swirlds.streamloader.util.Utils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OutputHandler that write json files for testing
 */
public class AvroFileOutputHandler implements OutputHandler<GenericRecord> {
	private final static int MAX_FIELDS_PER_FILE = 1_000_000;
	private final Path dataDir = Path.of("build/OUTPUT_DATA");
	public final Map<String, FileSet> fileSets = new HashMap<>();

	public AvroFileOutputHandler() {
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
	public synchronized void accept(final List<GenericRecord> records) {
		if (records != null) {
			for (var record : records) {
				if (record != null) {
					try {
						final String topicTableName = record.getSchema().getName();
						FileSet fileSet = fileSets.computeIfAbsent(topicTableName,
								name -> new FileSet(dataDir, record.getSchema()));
						fileSet.writeRecord(record);
					} catch (IOException e) {
						Utils.failWithError(e);
					}
				}
			}
		}
	}

	@Override
	public synchronized void close() throws Exception {
		System.out.println("FileOutputHandler.close");
		for(var fileSet: fileSets.values()) {
			fileSet.writer.flush();
			fileSet.writer.close();
		}
	}

	private static class FileSet {
		private final int maxRecords;
		private final Path dataDir;
		private final Schema schema;
		private DataFileWriter<GenericRecord> writer;
		private int count = 0;
		private int fileNumber = 0;

		private FileSet(Path dataDir, Schema schema) {
			this.dataDir = dataDir;
			this.schema = schema;
			maxRecords = MAX_FIELDS_PER_FILE /schema.getFields().size();
			try {
				openNewFile();
			} catch (IOException e) {
				Utils.failWithError(e);
			}
		}

		public void openNewFile() throws IOException {
			writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
			File newFile = dataDir.resolve(schema.getName() + "_" + (++fileNumber) + ".avro").toFile();
			writer.create(schema,newFile);
		}

		public void writeRecord(GenericRecord record) throws IOException {
			writer.append(record);
			if ((++count) >= maxRecords) {
				count = 0;
				writer.close();
				openNewFile();
			}
		}
	}
}
