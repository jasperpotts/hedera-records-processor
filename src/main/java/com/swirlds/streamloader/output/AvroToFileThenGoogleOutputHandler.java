package com.swirlds.streamloader.output;

import com.swirlds.streamloader.util.ByteCounterOutputStream;
import com.swirlds.streamloader.util.GoogleStorageHelper;
import com.swirlds.streamloader.util.Utils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import static com.swirlds.streamloader.util.GoogleStorageHelper.compressAndUploadFile;

/**
 * OutputHandler that write AVRO.gz files to local directory then has a pool of threads uploading to GCP
 */
public class AvroToFileThenGoogleOutputHandler implements OutputHandler<GenericRecord> {
	private final static long MAX_FILE_SIZE = 400*1024*1024*2; // 400MB TODO why do I have to x2 to make it work?
	private final static long MAX_CONSENSUS_TIME_PER_FILE = 30*86_400_000_000_000L; // 30 days in nanoseconds
	private final ConcurrentHashMap<String, FileSet> fileSets = new ConcurrentHashMap<>();
	private final ThreadPoolExecutor uploadExecutor;
	private final Path localDirectory;
	private final String bucketName;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	public AvroToFileThenGoogleOutputHandler(final Path localDirectory, final String bucketName) {
		this.localDirectory = localDirectory;
		this.bucketName = bucketName;

		// create a thread pool for uploading
		final ThreadGroup threadGroup = new ThreadGroup("gcp-uploaders");
		final AtomicLong threadCount = new AtomicLong();
		uploadExecutor = new ThreadPoolExecutor(
				Runtime.getRuntime().availableProcessors()/4,Runtime.getRuntime().availableProcessors()/2,
				30, TimeUnit.SECONDS,
				new ArrayBlockingQueue<>(100_000),
				runnable -> {
					Thread thread = new Thread(threadGroup, runnable, "gcp-uploader-"+threadCount.incrementAndGet());
					thread.setDaemon(true);
					return thread;
				});
	}

	@Override
	public void accept(final List<GenericRecord> records) {
		if (records != null && !records.isEmpty()) {
			final Schema schema = records.get(0).getSchema();
			fileSets
					.computeIfAbsent(schema.getName(),name -> new FileSet(schema))
					.addRecordsToInputQueue(records);
		}
	}

	@Override
	public void close() {
		System.out.println("AvroToFileThenGoogleOutputHandler.close");
		closed.set(true);
	}

	/**
	 * A set of files for one table, has its own input queue and thread for writing to files
	 */
	private class FileSet {
		private final ArrayBlockingQueue<List<GenericRecord>> inputQueue = new ArrayBlockingQueue<>(100_000);
		private final Schema schema;
		private final Path localDirectoryForFileSet;
		private final String consensusTimestampFieldName;
		private DataFileWriter<GenericRecord> writer;
		private ByteCounterOutputStream outputStream;
		private long startOfFileConsensusTimestamp = 0;
		private int fileNumber = 0;
		private String currentFileName;
		private Path currentFilePath;

		private FileSet(final Schema schema) {
			this.localDirectoryForFileSet = localDirectory.resolve(schema.getName());
			try {
				Files.createDirectories(this.localDirectoryForFileSet);
			} catch (IOException e) {
				Utils.failWithError(e);
			}
			this.schema = schema;
			if (schema.getField("consensus_timestamp") != null) {
				consensusTimestampFieldName = "consensus_timestamp";
			} else {
				consensusTimestampFieldName = "consensus_start_timestamp";
			}
			// open a file to start with
			openNewFile();
			// start processing thread
			new Thread(this::processQueueLoop, schema.getName()+"-file-writer").start();
		}

		/** File handling main loop, taking records of input queue and writing them to files */
		private void processQueueLoop() {
			while(!closed.get()) {
				try {
					List<GenericRecord> records = inputQueue.take();
					for(var record:records) {
						try {
							writer.append(record);
							final long consensusTime = (long) (Long) record.get(consensusTimestampFieldName);
							if (startOfFileConsensusTimestamp == 0) startOfFileConsensusTimestamp = consensusTime;
							if (outputStream.getCount() >= MAX_FILE_SIZE ||
									((outputStream.getCount() > 0) && (consensusTime - startOfFileConsensusTimestamp) >= MAX_CONSENSUS_TIME_PER_FILE)) {
								startOfFileConsensusTimestamp = consensusTime;
								closeFile();
								openNewFile();
							}
						} catch (IOException e) {
							Utils.failWithError(e);
						}
					}
				} catch (InterruptedException e) {
					Utils.failWithError(e);
				}
			}
			closeFile();
		}

		public void addRecordsToInputQueue(List<GenericRecord> records) {
			try {
				inputQueue.put(records);
			} catch (InterruptedException e) {
				Utils.failWithError(e);
			}
		}

		private void openNewFile() {
			try {
				currentFileName = schema.getName() + "_" + (++fileNumber) + ".avro";
				currentFilePath = localDirectoryForFileSet.resolve(currentFileName);
				outputStream = new ByteCounterOutputStream(
						new BufferedOutputStream(Files.newOutputStream(currentFilePath, StandardOpenOption.CREATE_NEW),
								1024*1024*32));//32MB
				writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))
						.create(schema, outputStream);
			} catch (IOException e) {
				Utils.failWithError(e);
			}
		}

		private void closeFile() {
			try {
				writer.flush();
				writer.close();
				outputStream.flush();
				outputStream.close();
				// capture file in local finals so lambda has them at this point in time
				final Path fileToUploadPath = currentFilePath;
				final String fileToUploadName = currentFileName;
				// schedule to be compressed and uploaded
				uploadExecutor.execute(() -> {
					compressAndUploadFile(bucketName, fileToUploadPath, schema.getName()+"/"+fileToUploadName+".gz");
				});
			} catch (IOException e) {
				Utils.failWithError(e);
			}
		}
	}
}
