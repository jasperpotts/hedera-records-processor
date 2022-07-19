package com.swirlds.streamloader.processing;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.SignedTransaction;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionID;
import com.hederahashgraph.api.proto.java.TransactionReceipt;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.BalanceChange;
import com.swirlds.streamloader.data.PartProcessedRecordFile;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.util.Utils;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;
import static com.swirlds.streamloader.util.Utils.readByteBufferSlice;
import static com.swirlds.streamloader.util.Utils.toHex;

@SuppressWarnings("DuplicatedCode")
public class ParallelRecordFileProcessor {
	public static final int HASH_OBJECT_SIZE_BYTES = 8+4+4+4+48;
	public static PartProcessedRecordFile processRecordFile(RecordFile recordFile) throws Exception {
		ByteBuffer dataBuf = recordFile.data().rewind();
		int fileVersionNumber = dataBuf.getInt();
		if (fileVersionNumber == 2) {
			return processRecordFileV2(recordFile, dataBuf);
		} else if (fileVersionNumber == 5) {
			return processRecordFileV5(recordFile, dataBuf);
		} else {
			return null;
		}
	}

	public static PartProcessedRecordFile processRecordFileV2(RecordFile recordFile, ByteBuffer dataBuf) {
		final int hapiVersion = dataBuf.getInt();
		final byte prevFileHashMarker = dataBuf.get();
		assert prevFileHashMarker == 1;
		byte[] prevFileHash = new byte[48];
		dataBuf.get(prevFileHash);
		// ==== HANDLE TRANSACTIONS
		List<JsonObject> transactionRows = new ArrayList<>();
		List<BalanceChange> balanceChanges = new ArrayList<>();
		long startConsensusTime = 0;
		long endConsensusTime = 0;
		long gasUsed = 0;
		long transactionIndex = 0;
		while (dataBuf.remaining() > 0) {
			final byte recordMarker = dataBuf.get();
			assert recordMarker == 2;
			final int lengthOfTransaction = dataBuf.getInt();
			final var transactionData = readByteBufferSlice(dataBuf,lengthOfTransaction);
			final int lengthOfTransactionRecord = dataBuf.getInt();
			final var transactionRecordData = readByteBufferSlice(dataBuf,lengthOfTransactionRecord);
			final var transactionRow = Json.createObjectBuilder();
			endConsensusTime = handleRecordTransaction(transactionRecordData, transactionData,
					transactionRow, balanceChanges, transactionIndex);
			if (transactionIndex == 0) {
				startConsensusTime = endConsensusTime;
			}
			transactionRows.add(transactionRow.build());
			transactionIndex ++;
		}
		// ==== CREATE RECORD ROW JSON
		JsonObject recordFileRow = createRecordFileRow(
				startConsensusTime, endConsensusTime, recordFile,
				"0."+hapiVersion+".0", transactionRows.size(),gasUsed);
		return new PartProcessedRecordFile(startConsensusTime,transactionRows, recordFileRow, balanceChanges);
	}

	public static PartProcessedRecordFile processRecordFileV5(RecordFile recordFile, ByteBuffer dataBuf) {
		final int hapiVersionMajor = dataBuf.getInt();
		final int hapiVersionMinor = dataBuf.getInt();
		final int hapiVersionPatch = dataBuf.getInt();
		final int objectStreamVersion = dataBuf.getInt();
		byte[] startObjectRunningHash = processHashObjectV5(dataBuf);
		// ==== HANDLE TRANSACTIONS
		List<JsonObject> transactionRows = new ArrayList<>();
		List<BalanceChange> balanceChanges = new ArrayList<>();
		long startConsensusTime = 0;
		long endConsensusTime = 0;
		long gasUsed = 0;
		long transactionIndex = 0;
		while (dataBuf.remaining() > HASH_OBJECT_SIZE_BYTES) {
			final long classId = dataBuf.getLong();
			assert classId == 0xe370929ba5429d8bL;
			final int classVersion = dataBuf.getInt();
			assert classVersion == 1;
			final int lengthOfTransactionRecord = dataBuf.getInt();
			final var transactionRecordData = readByteBufferSlice(dataBuf,lengthOfTransactionRecord);
			final int lengthOfTransaction = dataBuf.getInt();
			final var transactionData = readByteBufferSlice(dataBuf,lengthOfTransaction);

			final var transactionRow = Json.createObjectBuilder();
			endConsensusTime = handleRecordTransaction(transactionRecordData, transactionData, transactionRow,
					balanceChanges, transactionIndex);
			if (transactionIndex == 0) {
				startConsensusTime = endConsensusTime;
			}
			transactionRows.add(transactionRow.build());
			transactionIndex ++;
		}
		byte[] endObjectRunningHash = processHashObjectV5(dataBuf);
		// ==== CREATE RECORD ROW JSON
		JsonObject recordFileRow = createRecordFileRow(startConsensusTime, endConsensusTime, recordFile,
				hapiVersionMajor+"."+hapiVersionMinor+"."+hapiVersionPatch,transactionRows.size(),gasUsed);
		return new PartProcessedRecordFile(startConsensusTime,transactionRows, recordFileRow, balanceChanges);
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

	/**
	 * Create a json row for a single record file
	 */
	private static JsonObject createRecordFileRow(long consensusStartTimestamp, long consensusEndTimestamp,
			RecordFile recordFile, String hapiVersion, long count, long gasUsed) {
		try {
			return Json.createObjectBuilder()
					.add("consensus_start_timestamp", Long.toString(consensusStartTimestamp))
					.add("consensus_end_timestamp", Long.toString(consensusEndTimestamp))
					.add("data_hash", Utils.toHex(recordFile.hashOfThisFile()))
					.add("prev_hash", Utils.toHex(recordFile.hashOfPrevFile().get()))
					.add("number", Long.toString(recordFile.fileNumber()))
					.add("address_books", Json.createArrayBuilder().build())
					.add("signature_files", Json.createArrayBuilder().build())
					.add("signature_files", Json.createArrayBuilder().build())
					.add("fields", Json.createObjectBuilder()
							.add("count", Long.toString(count))
							.add("gas_used", Long.toString(gasUsed))
							.add("hapi_version", hapiVersion)
							.add("logs_bloom", "null")
							.add("name", recordFile.fileName())
							.add("size", Long.toString(recordFile.sizeBytes()))
							.build())
					.build();
		} catch (InterruptedException | ExecutionException e) {
			throw new RuntimeException(e);
		}

//		{"consensus_start_timestamp":"1657630724229567246",
//				"consensus_end_timestamp":"1657630725127611084",
//				"data_hash":"66313232",
//				"prev_hash":"36336364",
//				"number":66149,
//				"address_books":[ ],
//			    "signature_files":[ ],
//			    "fields":{
//					"count":"3",
//					"gas_used":"0",
//					"hapi_version":"0.26",
//					"logs_bloom":"null",
//					"name":"2022-07-12T12_58_44.229567246Z.rcd",
//					"size":"1788"
//				} }
	}

	public static long handleRecordTransaction(ByteBuffer transactionRecordData, ByteBuffer transactionData,
			JsonObjectBuilder transactionRow, List<BalanceChange> balanceChanges, long transactionIndex) {
		try {
			// parse proto buf messages
			final var transactionRecordMessage =
					TransactionRecord.parseFrom(transactionRecordData);
			final var transactionRoot = Transaction.parseFrom(transactionData);
			// handle the 3 ways that transaction body has been stored in file over time
			final TransactionBody transactionMessage;
			if (transactionRoot.hasBody()) {
				transactionMessage = transactionRoot.getBody();
			} else {
				ByteString bodyBytes = transactionRoot.getBodyBytes();
				if (bodyBytes.isEmpty()) {
					final var signedTransactionBytes = transactionRoot.getSignedTransactionBytes();
					bodyBytes = SignedTransaction.parseFrom(signedTransactionBytes).getBodyBytes();
				}
				transactionMessage = TransactionBody.parseFrom(bodyBytes);
			}
			// extract consensus time stamp
			final long consensusTimestampNanosLong =
					getEpocNanosAsLong(
							transactionRecordMessage.getConsensusTimestamp().getSeconds() ,
							transactionRecordMessage.getConsensusTimestamp().getNanos());
			// scan transfers list
			final JsonArrayBuilder transfersHbar = Json.createArrayBuilder();
			final TreeSet<Long> idSet = new TreeSet<>();
			for(AccountAmount amount : transactionRecordMessage.getTransferList().getAccountAmountsList()) {
				idSet.add(amount.getAccountID().getAccountNum());
				transfersHbar.add(Json.createObjectBuilder()
						.add("account", accountIdToString(amount.getAccountID()))
						.add("account_shard", Long.toString(amount.getAccountID().getShardNum()))
						.add("account_realm", Long.toString(amount.getAccountID().getRealmNum()))
						.add("account_number", Long.toString(amount.getAccountID().getAccountNum()))
						.add("amount", Long.toString(amount.getAmount()))
						.add("is_approval", amount.getIsApproval())
						.build()
				);
			}
			// build ids array from set of ids
			JsonArrayBuilder ids = Json.createArrayBuilder();
			idSet.forEach(ids::add);
			// build JSON row
			transactionRow
					.add("entityId",transactionReceiptToEntityNumber(transactionRecordMessage.getReceipt()))
					.add("type", transactionMessage.getDataCase().toString())
					.add("index", transactionIndex)
					.add("result", transactionRecordMessage.getReceipt().getStatus().toString())
					.add("scheduled",Boolean.toString(transactionMessage.getTransactionID().getScheduled()).toLowerCase())
					.add("nonce",Integer.toString(transactionMessage.getTransactionID().getNonce()))
					.add("transaction_id",transactionIdToString(transactionMessage.getTransactionID()))
					.add("fields",Json.createObjectBuilder()
							.add("payer_account_id", accountIdToString(transactionRecordMessage.getTransactionID().getAccountID()))
							.add("node", accountIdToString(transactionMessage.getNodeAccountID()))
							.add("valid_start_ns", "")
							.add("valid_duration_seconds", transactionMessage.getTransactionValidDuration().getSeconds())
							.add("initial_balance", "")
							.add("max_fee", transactionMessage.getTransactionFee())
							.add("charged_tx_fee", transactionRecordMessage.getTransactionFee())
							.add("memo", transactionMessage.getMemo())
							.add("transaction_hash", toHex(transactionRecordMessage.getTransactionHash().toByteArray()))
							.add("transaction_bytes", toHex(transactionMessage.toByteArray()))
							.add("parent_consensus_timestamp", "null")
							.add("errata", "")
					)
					.add("consensus_timestamp",Long.toString(consensusTimestampNanosLong))
					.add("transfers_hbar",transfersHbar.build())
					.add("ids",ids.build());
			return consensusTimestampNanosLong;
		} catch (InvalidProtocolBufferException e) {
			throw new RuntimeException(e);
		}
	}

	private static String accountIdToString(AccountID id) {
		return id.getShardNum() + "." + id.getRealmNum() + "." + id.getAccountNum();
	}
	private static String transactionIdToString(TransactionID id) {
		// example "0.0.55492-1657630711811-5098000"
		return id.getAccountID().getShardNum() + "." + id.getAccountID().getRealmNum() + "." +
				id.getAccountID().getAccountNum() + "-" + id.getTransactionValidStart().getSeconds() +
				"-" + id.getTransactionValidStart().getNanos();
	}

	private static String transactionReceiptToEntityNumber(TransactionReceipt receipt) {
		if (receipt.hasAccountID()) {
			return accountIdToString(receipt.getAccountID());
		} else if (receipt.hasFileID()) {
			return receipt.getFileID().getShardNum() + "." + receipt.getFileID().getRealmNum() + "." + receipt.getFileID().getFileNum();
		} else if (receipt.hasContractID()) {
			return receipt.getContractID().getShardNum() + "." + receipt.getContractID().getRealmNum() + "." + receipt.getContractID().getContractNum();
		} else if (receipt.hasTokenID()) {
			return receipt.getTokenID().getShardNum() + "." + receipt.getTokenID().getRealmNum() + "." + receipt.getTokenID().getTokenNum();
		} else if (receipt.hasTopicID()) {
			return receipt.getTopicID().getShardNum() + "." + receipt.getTopicID().getRealmNum() + "." + receipt.getTopicID().getTopicNum();
		} else if (receipt.hasScheduleID()) {
			return receipt.getScheduleID().getShardNum() + "." + receipt.getScheduleID().getRealmNum() + "." + receipt.getScheduleID().getScheduleNum();
		} else {
			return "-1";
		}
	}
}

// example JSON
//			{
//				"entityId":"0",
//				"type":"CRYPTOTRANSFER",
//				"index":"2",
//				"result":"22",
//				"scheduled":"false",
//				"nonce":"0",
//				"transaction_id":"0.0.55492-1657630711811-5098000",
//				"fields":{
//					"payer_account_id":"0.0.55492",
//					"node":"0.0.13",
//					"valid_start_ns":"1657630711811005098",
//					"valid_duration_seconds":"120",
//					"initial_balance":"0",
//					"max_fee":"100000000",
//					"charged_tx_fee":"167450",
//					"memo":"",
//					"transaction_hash":"1RXIqwKu/Spfx0K41HCKxJ22r49bUKa+0eLzQhg51P41IREmXPosUJ3J1slyqaVu",
//					"transaction_bytes":"KrUBCksKGgoMCPfftZYGEKrp24IDEggIABAAGMSxAxgAEgYIABAAGA0YgMLXLyICCHhyHAoaCgwKCAgAEAAYxLEDEDEKCgoGCAAQABgNEDISZgpkCiA+R1A7HmCExywF",
//					"parent_consensus_timestamp":"null",
//					"errata":""
//				},
//				"consensus_timestamp":"1657630725127611084",
//				"transfers_hbar": [
//						{ "account":"0.0.13", "account_shard":"0", "account_realm":"0", "account_number":"13", "amount":6807, "is_approval":false },
//						{ "account":"0.0.98", "account_shard":"0", "account_realm":"0", "account_number":"98", "amount":160668, "is_approval":false },
//						{ "account":"0.0.55492", "account_shard":"0", "account_realm":"0", "account_number":"55492", "amount":-167475, "is_approval":false }
//				],
//				"ids":[ 13, 98, 55492 ]
//			}

// example mirror node query
//{"transactions":[
//		{"bytes":null,
//		"charged_tx_fee":256720,
//		"consensus_timestamp":"1657920010.421006935",
//		"entity_id":"0.0" +".120438",
//		"max_fee":"4000000",
//		"memo_base64":"",
//		"name":"CONSENSUSSUBMITMESSAGE",
//		"node":"0.0.16",
//		"nonce":0,
//		"parent_consensus_timestamp":null,
//		"result":"SUCCESS",
//		"scheduled":false,
//		"transaction_hash": "zFgxH4yA1Ruo377XGgd4CD97IwsV5GWrVHwId8JzvW9KT0UXk0W8k/h77103FDLi",
//		"transaction_id":"0.0.41099-1657920000-150888630",
//		"transfers":[{"account":"0.0.16","amount":8530,"is_approval":false},{"account":"0.0.98","amount":
//		248190,"is_approval":false},{"account":"0.0.41099","amount":-256720,"is_approval":false}],
//		"valid_duration_seconds":"120",
//		"valid_start_timestamp":"1657920000.150888630"}]}