package com.swirlds.streamloader.processing;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.util.GoogleStorageHelper;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Optional;

import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;
import static com.swirlds.streamloader.util.Utils.hashShar384;
import static com.swirlds.streamloader.util.Utils.readByteBufferSlice;

/**
 * Takes URLs and outputs loaded and parsed record file
 */
public class RecordFileDownloaderBlock extends PipelineBlock.Parallel<URL, RecordFile> {
	public static final int HASH_OBJECT_SIZE_BYTES = 8+4+4+4+48;

	public RecordFileDownloaderBlock(PipelineLifecycle pipelineLifecycle) {
		super("record-processor", pipelineLifecycle,  (int)Math.min(96,Runtime.getRuntime().availableProcessors()*1.5));
	}

	@Override
	public RecordFile processDataItem(final URL url) throws URISyntaxException, IOException {
		// download the data
		ByteBuffer data;
		if (url.getProtocol().equals("gs")) {
			data = GoogleStorageHelper.downloadBlob(url);
		} else if (url.getProtocol().equals("file")) {
			data = ByteBuffer.wrap(Files.readAllBytes(Path.of(url.toURI())));
		} else {
			throw new IllegalArgumentException("Unknown URL protocol for "+url.getProtocol()+" ["+url+"]");
		}
		// parse file
		return parseRecordFile(data, url.getFile());
	}

	private RecordFile parseRecordFile(ByteBuffer data, String fileName) {
		data.rewind();
		final int size = data.remaining();
		final byte[] fileHash = hashShar384(data);
		data.rewind();
		int fileVersionNumber = data.getInt();
		return switch (fileVersionNumber) {
			case 2 -> processRecordFileV2(data, fileVersionNumber, fileName, size, fileHash);
			case 5 -> processRecordFileV5(data, fileVersionNumber, fileName, size, fileHash);
			default -> throw new IllegalArgumentException(
					"Encountered file with unsupported version number of [" + fileVersionNumber + "] - SKIPPING!");
		};
	}

	public static RecordFile processRecordFileV2(final ByteBuffer dataBuf, int fileVersion,
			String fileName, int fileSize, byte[] fileHash) {
		final int hapiVersion = dataBuf.getInt();
		final byte prevFileHashMarker = dataBuf.get();
		assert prevFileHashMarker == 1;
		byte[] prevFileHash = new byte[48];
		dataBuf.get(prevFileHash);
		// ==== HANDLE TRANSACTIONS
		ArrayList<Transaction> transactions = new ArrayList<>();
		ArrayList<TransactionRecord> transactionRecords = new ArrayList<>();
		long startConsensusTime = 0;
		long endConsensusTime = 0;
		while (dataBuf.remaining() > 0) {
			final byte recordMarker = dataBuf.get();
			assert recordMarker == 2;
			final int lengthOfTransaction = dataBuf.getInt();
			final var transactionData = readByteBufferSlice(dataBuf,lengthOfTransaction);
			final int lengthOfTransactionRecord = dataBuf.getInt();
			final var transactionRecordData = readByteBufferSlice(dataBuf,lengthOfTransactionRecord);
			try {
				// parse protobuf messages
				final Transaction transaction = Transaction.parseFrom(transactionData);
				final TransactionRecord transactionRecord = TransactionRecord.parseFrom(transactionRecordData);
				transactionRecords.add(transactionRecord);
				transactions.add(transaction);
				// collect consensus times
				endConsensusTime = getEpocNanosAsLong(transactionRecord.getConsensusTimestamp());
				if (startConsensusTime == 0) startConsensusTime = endConsensusTime;
			} catch (InvalidProtocolBufferException e) {
				throw new RuntimeException(e);
			}
		}

		return new RecordFile(
				dataBuf,
				fileVersion,
				"0."+hapiVersion+".0",
				fileSize,
				fileHash,
				Optional.of(prevFileHash),
				fileName,
				transactions.toArray(Transaction[]::new),
				transactionRecords.toArray(TransactionRecord[]::new),
				null,
				null,
				startConsensusTime,
				endConsensusTime);
	}

	public static RecordFile processRecordFileV5(final ByteBuffer dataBuf, int fileVersion,
			String fileName, int fileSize, byte[] fileHash) {
		final int hapiVersionMajor = dataBuf.getInt();
		final int hapiVersionMinor = dataBuf.getInt();
		final int hapiVersionPatch = dataBuf.getInt();
		final int objectStreamVersion = dataBuf.getInt();
		byte[] startObjectRunningHash = processHashObjectV5(dataBuf);
		// ==== HANDLE TRANSACTIONS
		ArrayList<Transaction> transactions = new ArrayList<>();
		ArrayList<TransactionRecord> transactionRecords = new ArrayList<>();
		long startConsensusTime = 0;
		long endConsensusTime = 0;
		while (dataBuf.remaining() > HASH_OBJECT_SIZE_BYTES) {
			final long classId = dataBuf.getLong();
			assert classId == 0xe370929ba5429d8bL;
			final int classVersion = dataBuf.getInt();
			assert classVersion == 1;
			final int lengthOfTransactionRecord = dataBuf.getInt();
			final var transactionRecordData = readByteBufferSlice(dataBuf,lengthOfTransactionRecord);
			final int lengthOfTransaction = dataBuf.getInt();
			final var transactionData = readByteBufferSlice(dataBuf,lengthOfTransaction);
			// parse proto buf messages
			try {
				// parse protobuf messages
				final Transaction transaction = Transaction.parseFrom(transactionData);
				final TransactionRecord transactionRecord = TransactionRecord.parseFrom(transactionRecordData);
				transactionRecords.add(transactionRecord);
				transactions.add(transaction);
				// collect consensus times
				endConsensusTime = getEpocNanosAsLong(transactionRecord.getConsensusTimestamp());
				if (startConsensusTime == 0) startConsensusTime = endConsensusTime;
			} catch (InvalidProtocolBufferException e) {
				throw new RuntimeException(e);
			}
		}
		byte[] endObjectRunningHash = processHashObjectV5(dataBuf);
		return new RecordFile(
				dataBuf,
				fileVersion,
				hapiVersionMajor+"."+hapiVersionMinor+"."+hapiVersionPatch,
				fileSize,
				fileHash,
				Optional.empty(),
				fileName,
				transactions.toArray(Transaction[]::new),
				transactionRecords.toArray(TransactionRecord[]::new),
				startObjectRunningHash,
				endObjectRunningHash,
				startConsensusTime,
				endConsensusTime);
	}

	/** Read a V5 stream running hash from buffer */
	public static byte[] processHashObjectV5(ByteBuffer dataBuf) {
		final long classId = dataBuf.getLong();
		assert classId == 0xf422da83a251741eL;
		final int classVersion = dataBuf.getInt();
		assert classVersion == 1;
		final int digestType = dataBuf.getInt();
		assert digestType == 0x58ff811b; // SHA-384
		final int hashLength = dataBuf.getInt();
		assert hashLength == 48;
		byte[] hash = new byte[hashLength];
		dataBuf.get(hash);
		return hash;
	}
}
