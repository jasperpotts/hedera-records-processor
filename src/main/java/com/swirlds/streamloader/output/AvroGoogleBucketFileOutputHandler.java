package com.swirlds.streamloader.output;

import com.swirlds.streamloader.util.GoogleStorageHelper;
import com.swirlds.streamloader.util.Utils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OutputHandler that write json files for testing
 */
public class AvroGoogleBucketFileOutputHandler implements OutputHandler<GenericRecord> {
	private final static int MAX_FIELDS_PER_FILE = 1_000_000;
	private final Map<String, FileSet> fileSets = new HashMap<>();
	private final String bucketName;

	public AvroGoogleBucketFileOutputHandler(final String bucketName) {
		this.bucketName = bucketName;
	}

	@Override
	public synchronized void accept(final List<GenericRecord> records) {
		if (records != null) {
			for (var record : records) {
				if (record != null) {
					try {
						FileSet fileSet = fileSets.computeIfAbsent(record.getSchema().getName(),
								name -> new FileSet(record.getSchema(), bucketName));
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
			fileSet.closeFile();
		}
	}

	private static class FileSet {
		private final int maxRecords;
		private final Schema schema;
		private final String bucketName;
		private DataFileWriter<GenericRecord> writer;
		private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4*1024*1024);
		private int count = 0;
		private int fileNumber = 0;

		private FileSet(Schema schema, String bucketName) {
			this.schema = schema;
			this.bucketName = bucketName;
			maxRecords = MAX_FIELDS_PER_FILE /schema.getFields().size();
			try {
				openNewFile();
			} catch (IOException e) {
				Utils.failWithError(e);
			}
		}

		public void openNewFile() throws IOException {
			writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
			outputStream.reset();
			writer.create(schema,outputStream);
		}

		public void writeRecord(GenericRecord record) throws IOException {
			writer.append(record);
			if ((++count) >= maxRecords) {
				count = 0;
				closeFile();
				openNewFile();
			}
		}

		public void closeFile() throws IOException {
			writer.flush();
			writer.close();
			outputStream.flush();
			String path = schema.getName() +"/" + schema.getName() + "_" + (++fileNumber) + ".avro";
			GoogleStorageHelper.uploadBlob(bucketName, path, outputStream.toByteArray());
		}
	}
}
