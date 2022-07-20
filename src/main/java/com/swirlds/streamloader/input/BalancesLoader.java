package com.swirlds.streamloader.input;

import com.hedera.services.stream.proto.AllAccountBalances;
import com.swirlds.streamloader.util.ByteBufferInputStream;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static com.swirlds.streamloader.util.Utils.TINY_BAR_IN_HBAR;
import static com.swirlds.streamloader.util.Utils.getEpocNanosFromFileName;

/**
 * Class for loading balances files
 */
public class BalancesLoader {
	public static final String INITIAL_BALANCE_FILE_NAME = "2019-09-13T22_00_00.000081Z_Balances.csv";
	public static final String INITIAL_BALANCE_FILE_TIMESTAMP = getEpocNanosFromFileName(INITIAL_BALANCE_FILE_NAME);

	/**
	 * Loads a CSV or Protobuf formatted balances file
	 *
	 * @param balancesFile path to balances file in CSV format
	 * @return map of account to balance
	 */
	public static LongLongHashMap loadBalances(Path balancesFile) {
		final String fileName = balancesFile.getFileName().toString();
		System.out.println("fileName = " + fileName);
		try {
			if (fileName.endsWith(".pb.gz")) {
					return loadBalancesProtobuf(ByteBuffer.wrap(Files.readAllBytes(balancesFile)));
			} else if (fileName.endsWith(".csv")) {
				return loadBalancesCSV(ByteBuffer.wrap(Files.readAllBytes(balancesFile)));
			} else {
				throw new IllegalArgumentException("File should be CSV or protobuf");
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
	/**
	 * Loads a CSV or Protobuf formatted balances file
	 *
	 * @param balancesFileContents byte buffer containing contents of balances file to load
	 * @param fileName name of balances file
	 * @return map of account to balance
	 */
	public static LongLongHashMap loadBalances(ByteBuffer balancesFileContents, String fileName) {
		System.out.println("fileName = " + fileName);
		if (fileName.endsWith(".pb.gz")) {
			return loadBalancesProtobuf(balancesFileContents);
		} else if (fileName.endsWith(".csv")) {
			return loadBalancesCSV(balancesFileContents);
		} else {
			throw new IllegalArgumentException("File should be CSV or protobuf");
		}
	}

	/**
	 * Loads a CSV formatted balances file
	 *
	 * @param balancesFileContents path to balances file in CSV format
	 * @return map of account to balance
	 */
	private static LongLongHashMap loadBalancesCSV(final ByteBuffer balancesFileContents) {
		final LongLongHashMap balancesMap = new LongLongHashMap();
		final String contents;
		if (balancesFileContents.hasArray()) {
			contents = new String(balancesFileContents.array(), StandardCharsets.UTF_8);
		} else {
			byte[] array = new byte[balancesFileContents.remaining()];
			balancesFileContents.get(array);
			contents = new String(array, StandardCharsets.UTF_8);
		}
		contents.lines().forEach(line -> {
			if (line.startsWith("TimeStamp:")) {
				System.out.println("line = " + line);
			} else if (line.startsWith("shardNum")) {
				if (!line.equals("shardNum,realmNum,accountNum,balance")) {
					throw new IllegalArgumentException("Unknown CSV file encountered with columns: " + line);
				}
			} else {
				//0,0,19,10000000000
				final var columns = line.split(",");
				balancesMap.put(Long.parseLong(columns[2]), Long.parseLong(columns[3]));
			}
		});
		return balancesMap;
	}

	/**
	 * Loads a protobuf formatted balances file
	 *
	 * @param balancesFileContents path to balances file in CSV format
	 * @return map of account to balance
	 */
	private static LongLongHashMap loadBalancesProtobuf(final ByteBuffer balancesFileContents) {
		final LongLongHashMap balancesMap = new LongLongHashMap();
		try (final var input = new GZIPInputStream(new ByteBufferInputStream(balancesFileContents))) {
			final AllAccountBalances balances = AllAccountBalances.parseFrom(input);
			for (final var account : balances.getAllAccountsList()) {
				balancesMap.put(
						account.getAccountID().getAccountNum(),
						account.getHbarBalance()
				);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return balancesMap;
	}

	public static void main(String[] args) throws Exception {
		findBalanceStreams(Path.of("data/accountBalances/mainnet"))
				.map(BalancesLoader::loadBalances)
				.forEach(map -> {
					System.out.println("=========================================");
					System.out.println("map.size() = " + map.size());
					System.out.println("=========================================");
					for (int i = 1; i < 200; i++) {
						final long tinyBar = map.getIfAbsent(i,-1);
						if (tinyBar != -1) {
							final double hbar = (double)tinyBar / TINY_BAR_IN_HBAR;
							final double dollar = hbar * 0.229d;
							System.out.printf("%5d = %,30.5f h %,30.2f $\n", i, hbar, dollar);
						}
					}
				});
	}

	private static Stream<Path> findBalanceStreams(Path directory) throws Exception {
		return Files.find(directory, Integer.MAX_VALUE,
				(filePath, fileAttr) -> fileAttr.isRegularFile() && (
						filePath.getFileName().toString().endsWith(".csv") ||
								filePath.getFileName().toString().endsWith(".pb.gz"))
		);
	}

}