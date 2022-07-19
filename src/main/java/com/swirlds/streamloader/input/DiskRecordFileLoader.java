package com.swirlds.streamloader.input;

import com.swirlds.streamloader.util.PreCompletedFuture;
import com.swirlds.streamloader.util.Utils;
import com.swirlds.streamloader.data.RecordFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Scan a directory loading all record files
 */
public class DiskRecordFileLoader implements RecordFileLoader {
	private final Path recordFileDirectory;
	private final AtomicLong fileCount = new AtomicLong(0);
	private final AtomicReference<byte[]> prevFileHash = new AtomicReference<>(new byte[48]);

	public DiskRecordFileLoader(final Path recordFileDirectory) {
		this.recordFileDirectory = recordFileDirectory;
	}

	@Override
	public void startLoadingRecordFiles(final Consumer<RecordFile> recordFileConsumer) {
		new Thread(() -> {
			final long START = System.currentTimeMillis();
			try {
				findRecordStreams(recordFileDirectory)
						.forEach(file -> {
							try {
								final ByteBuffer dataBuf = Utils.readFileFully(file);
								final byte[] fileHash = Utils.hashShar384(dataBuf);
								final RecordFile recordFile = new RecordFile(
										dataBuf,
										fileCount.incrementAndGet(),
										Files.size(file),
										fileHash,
										new PreCompletedFuture<>(prevFileHash.getAndSet(fileHash)),
										file.getFileName().toString()
								);
								recordFileConsumer.accept(recordFile);
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						});
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				final var  took = Duration.ofMillis(System.currentTimeMillis() - START);
				System.out.println("took = " + took);
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
