package com.swirlds.streamloader.input;

import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.util.PipelineConsumer;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.swirlds.streamloader.input.BalancesLoader.INITIAL_BALANCE_FILE_NAME;

/**
 * Scan a directory loading all record files
 */
@SuppressWarnings("unused")
public class DiskFileLoader implements FileLoader {
	private final Path recordFileDirectory;

	public DiskFileLoader(final Path recordFileDirectory) {
		this.recordFileDirectory = recordFileDirectory;
	}

	@Override
	public void startLoadingRecordFileUrls(final PipelineConsumer<URL> consumer) {
		new Thread(() -> {
			try {
				Path[] recordFilePaths = findRecordStreams(recordFileDirectory).toArray(Path[]::new);
				System.out.println("Processing " + recordFilePaths.length+" files...");
				for (int i = 0; i < recordFilePaths.length; i++) {
					final Path recordFilePath = recordFilePaths[i];
					final boolean isLast = i == recordFilePaths.length-1;
					consumer.accept(recordFilePath.toUri().toURL(), isLast);
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

	@Override
	public ObjectLongHashMap<BalanceKey> loadInitialBalances() {
		return BalancesLoader.loadBalances(Path.of("test-data/accountBalances/mainnet/"+INITIAL_BALANCE_FILE_NAME));
	}
}
