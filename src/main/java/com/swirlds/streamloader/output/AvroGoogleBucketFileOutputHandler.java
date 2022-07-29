package com.swirlds.streamloader.output;

import com.swirlds.streamloader.util.GoogleStorageHelper;
import com.swirlds.streamloader.util.Utils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.swirlds.streamloader.StreamDownloaderMain.MAX_NUM_ROW_PER_FILE_MAP;

/**
 * OutputHandler that write json files for testing
 */
public class AvroGoogleBucketFileOutputHandler implements OutputHandler<GenericRecord> {
	private final static long DEFAULT_MAX_ROWS_PER_FILE = 100_000;
	private final ConcurrentHashMap<String, FileSet> fileSets = new ConcurrentHashMap<>();
	private final String bucketName;

	public AvroGoogleBucketFileOutputHandler(final String bucketName) {
		this.bucketName = bucketName;
	}

	@Override
	public void accept(final List<GenericRecord> records) {
		if (records != null && !records.isEmpty()) {
			final Schema schema = records.get(0).getSchema();
			fileSets
					.computeIfAbsent(schema.getName(),name -> new FileSet(schema, bucketName))
					.addRecords(records);
		}
	}

	@Override
	public void close() {
		System.out.println("AvroGoogleBucketFileOutputHandler.close");
		for(var fileSet: fileSets.values()) {
			fileSet.close();
		}
	}

	private static class FileSet {
		private final ArrayBlockingQueue<List<GenericRecord>> inputQueue = new ArrayBlockingQueue<>(100_000);
		private final AtomicBoolean closed = new AtomicBoolean(false);


		private FileSet(final Schema schema, final String bucketName) {
			FileCreateAndUploadThread fileCreateAndUploadThread = new FileCreateAndUploadThread(bucketName, schema,inputQueue,closed);
			fileCreateAndUploadThread.start();
		}

		public void addRecords(List<GenericRecord> records) {
			try {
				inputQueue.put(records);
			} catch (InterruptedException e) {
				Utils.failWithError(e);
			}
		}

		public void close() {
			closed.set(true);
		}
	}

	private static class FileCreateAndUploadThread extends Thread {
		private final Schema schema;
		private final ArrayBlockingQueue<List<GenericRecord>> inputQueue;
		private final AtomicBoolean closed;
		private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(4*1024*1024);
		private final long maxRecords;
		private final String bucketName;
		private DataFileWriter<GenericRecord> writer;
		private int count = 0;
		private int fileNumber = 0;
		public FileCreateAndUploadThread(final String bucketName, final Schema schema,
				final ArrayBlockingQueue<List<GenericRecord>> inputQueue, final AtomicBoolean closed) {
			super(schema.getName()+"-file-uploader");
			this.bucketName = bucketName;
			this.schema = schema;
			this.inputQueue = inputQueue;
			this.closed = closed;
			this.maxRecords = MAX_NUM_ROW_PER_FILE_MAP.getOrDefault(schema.getName(), DEFAULT_MAX_ROWS_PER_FILE);
			System.out.println("Creating files for ["+schema.getName()+"] max rows per file = "+maxRecords);
			try {
				openNewFile();
			} catch (IOException e) {
				Utils.failWithError(e);
			}
		}

		@Override
		public void run() {
			while(!closed.get()) {
				try {
					List<GenericRecord> records = inputQueue.take();
					for(var record:records) {
						try {
							writeRecord(record);
						} catch (IOException e) {
							Utils.failWithError(e);
						}
					}
				} catch (InterruptedException e) {
					Utils.failWithError(e);
				}
			}
			try {
				closeFile();
			} catch (IOException e) {
				Utils.failWithError(e);
			}
		}

		public void openNewFile() throws IOException {
			writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
			outputStream.reset();
			writer.create(schema,outputStream);
		}
		private void writeRecord(GenericRecord record) throws IOException {
			writer.append(record);
			if ((++count) >= maxRecords) {
				count = 0;
				closeFile();
				openNewFile();
			}
		}
		private void closeFile() throws IOException {
			writer.flush();
			writer.close();
			outputStream.flush();
			String path = schema.getName() +"/" + schema.getName() + "_" + (++fileNumber) + ".avro";
			GoogleStorageHelper.uploadBlob(bucketName, path, outputStream.toByteArray());
		}
	}
}
