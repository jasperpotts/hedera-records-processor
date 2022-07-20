package com.swirlds.streamloader.input;

import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.util.PreCompletedFuture;
import com.swirlds.streamloader.util.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Scan a directory loading all record files
 */
public class DiskRecordFileLoader implements RecordFileLoader {
	private final Path recordFileDirectory;
	private AtomicLong fileCount = new AtomicLong(0);
	private AtomicReference<byte[]> prevFileHash = new AtomicReference<>(new byte[48]);

	public DiskRecordFileLoader(final Path recordFileDirectory) {
		this.recordFileDirectory = recordFileDirectory;
	}

	@Override
	public void startLoadingRecordFiles(final ArrayBlockingQueue<Future<RecordFile>> recordFileQueue) {
		new Thread(() -> {
			try {
				Path[] recordFilePaths = findRecordStreams(recordFileDirectory).toArray(Path[]::new);
				System.out.println("Processing " + recordFilePaths.length+" files...");
				for (int i = 0; i < recordFilePaths.length; i++) {
					final Path file = recordFilePaths[i];
					try {
						final ByteBuffer dataBuf = Utils.readFileFully(file);
						final byte[] fileHash = Utils.hashShar384(dataBuf);
						final RecordFile recordFile = new RecordFile(
								i == recordFilePaths.length -1,
								dataBuf,
								fileCount.incrementAndGet(),
								Files.size(file),
								fileHash,
								new PreCompletedFuture<>(prevFileHash.getAndSet(fileHash)),
								file.getFileName().toString()
						);
						recordFileQueue.put(new PreCompletedFuture<>(recordFile));
					} catch (IOException | InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.flush();
				System.exit(1);
			}
		},"Record File Loader").start();
	}

	/**
	 * Scan a directory finding all record files
	 */
	private static Stream<Path> findRecordStreams(Path directory) throws Exception {
		return Files.find(directory, Integer.MAX_VALUE,
				(filePath, fileAttr) -> fileAttr.isRegularFile() && filePath.getFileName().toString().endsWith(".rcd")
		);
	}
}
