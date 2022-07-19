package com.swirlds.streamloader.input;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.util.PreCompletedFuture;
import com.swirlds.streamloader.util.Utils;

import javax.json.Json;
import javax.json.stream.JsonParser;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


public class GCPRecordFileLoader implements RecordFileLoader {
	private static final int cpuCores = Runtime.getRuntime().availableProcessors();
	// create gcp client
	private static final Storage STORAGE = StorageOptions.getDefaultInstance().getService();
	/** Max number of futures to queue */
	private static final int BATCH_SIZE = 10_000;
	public enum HederaNetwork {
		DEMO("hedera-demo-streams"),
		MAINNET("hedera-mainnet-streams"),
		TESTNET("hedera-stable-testnet-streams-2020-08-27"),
		PREVIEWNET("hedera-preview-testnet-streams");

		private final String bucketName;

		HederaNetwork(final String bucketName) {
			this.bucketName = bucketName;
		}

		public String getBucketName() {
			return bucketName;
		}
	}

	private final String gcpProjectName;
	private final HederaNetwork network;
	private final String nodeID;
	private final Date startDate;
	private final AtomicLong fileCount = new AtomicLong(0);

	public GCPRecordFileLoader(final HederaNetwork network, final String nodeID,
			final Date startDate) {
		this.network = network;
		this.nodeID = nodeID;
		this.startDate = startDate;
		// get the project name from credentials file
		final String googleCredentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
		if (googleCredentials == null || googleCredentials.length() == 0) {
			throw new RuntimeException("You need to set \"GOOGLE_APPLICATION_CREDENTIALS\" environment variable");
		}
		try {
			final JsonParser parser = Json.createParser(Files.newBufferedReader(Path.of(googleCredentials)));
				parser.next();
				var object = parser.getObject();
				this.gcpProjectName = object.getString("project_id",null);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		if (this.gcpProjectName == null) {
			throw new IllegalStateException(
					"Could not talk to GCP as can not read project id from GOOGLE_APPLICATION_CREDENTIALS");
		}
	}

	@SuppressWarnings({ "WhileLoopReplaceableByForEach", "BusyWait" })
	@Override
	public void startLoadingRecordFiles(final Consumer<RecordFile> recordFileConsumer) {
		// create pool of thread to do file downloading
		final ThreadGroup downloadingExecutorsThreadGroup = new ThreadGroup("downloaders");
		final AtomicLong downloadingThreadCount = new AtomicLong();
		final ExecutorService downloadingExecutorService = Executors.newFixedThreadPool(cpuCores,
				runnable -> new Thread(downloadingExecutorsThreadGroup, runnable, "downloader-"+downloadingThreadCount.incrementAndGet()));

		final ConcurrentLinkedDeque<Future<RecordFile>> futures = new ConcurrentLinkedDeque<>();
		final AtomicBoolean finishedListing = new AtomicBoolean(false);
		final Thread directoryLister = new Thread(() -> {
			try {
				final var iterableOverBlobs = listGcpDirectoryAsIterable("recordstreams/record" + nodeID + "/").iterator();
				Future<RecordFile> future = null;
				while(iterableOverBlobs.hasNext()) {
					if (futures.size() < BATCH_SIZE) {
						// get next blob to download
						final Blob recordFileBlob = iterableOverBlobs.next();
						// only process records files
						if (recordFileBlob.getName().endsWith(".rcd")) {
							final Future<RecordFile> finalFuture = future;
							// add a task to download and parse
							final Callable<RecordFile> newTask = () -> {
								// download
								final ByteBuffer dataBuf = downloadBlob(recordFileBlob);
								return new RecordFile(
										dataBuf,
										fileCount.incrementAndGet(),
										recordFileBlob.getSize(),
										Utils.hashShar384(dataBuf),
										(finalFuture == null) ? new PreCompletedFuture<>(new byte[48]) : new PrevHashFuture(finalFuture),
										recordFileBlob.getName()
								);
							};
							// schedule new download task to run and add it's future to end of list
							future = downloadingExecutorService.submit(newTask);
							futures.add(future);
						}
					} else { // list of futures is full
						Thread.sleep(1000);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			} finally {
				finishedListing.set(true);
			}
		}, "directoryLister");
		final Thread callbackInOrderThread = new Thread(() -> {
			try {
				while(!finishedListing.get()) {
					final Future<RecordFile> future = futures.poll();
					if (future != null) {
						try {
							recordFileConsumer.accept(future.get());
						} catch (ExecutionException e) {
							e.printStackTrace();
						}
					} else { // wait for downloaders to catch up
						Thread.sleep(1000);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}, "directoryLister");
		// start threads
		directoryLister.start();
		callbackInOrderThread.start();
	}

	public Iterable<Blob> listGcpDirectoryAsIterable(String gcpDirectoryPath) {
		// The name for the new bucket
		final String bucketName = network.getBucketName();
		// List the file in the bucket
		final var blobs = STORAGE.list(bucketName,
				Storage.BlobListOption.prefix(gcpDirectoryPath),
				Storage.BlobListOption.currentDirectory(),
				Storage.BlobListOption.userProject(gcpProjectName),
				Storage.BlobListOption.pageSize(10_000));
		return blobs.iterateAll();
	}

	public ByteBuffer downloadBlob(Blob blob) {
		return ByteBuffer.wrap(blob.getContent(Blob.BlobSourceOption.userProject(gcpProjectName)));
	}

	/**
	 * Future wrapper to wrap a record file future and return the file hash
	 */
	@SuppressWarnings("NullableProblems")
	private static class PrevHashFuture implements Future<byte[]> {
		private final Future<RecordFile> prevFileRecordFuture;

		public PrevHashFuture(final Future<RecordFile> prevFileRecordFuture) {
			this.prevFileRecordFuture = prevFileRecordFuture;
		}

		@Override
		public boolean cancel(final boolean mayInterruptIfRunning) {
			return false;
		}

		@Override
		public boolean isCancelled() {
			return prevFileRecordFuture.isCancelled();
		}

		@Override
		public boolean isDone() {
			return prevFileRecordFuture.isDone();
		}

		@Override
		public byte[] get() throws InterruptedException, ExecutionException {
			return prevFileRecordFuture.get().hashOfThisFile();
		}

		@Override
		public byte[] get(final long timeout, final TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			return prevFileRecordFuture.get(timeout, unit).hashOfThisFile();
		}
	}
}
