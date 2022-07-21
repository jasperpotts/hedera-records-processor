package com.swirlds.streamloader.processing;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.ContractFunctionResult;
import com.hederahashgraph.api.proto.java.ContractID;
import com.hederahashgraph.api.proto.java.ContractLoginfo;
import com.hederahashgraph.api.proto.java.NftTransfer;
import com.hederahashgraph.api.proto.java.SignedTransaction;
import com.hederahashgraph.api.proto.java.TokenTransferList;
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
import javax.json.JsonArray;
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

/**
 * Does all processing on the records stream that can be done in parallel. For example creating of transaction rows or
 * record file rows.
 */
@SuppressWarnings({ "DuplicatedCode", "unused", "deprecation" })
public class ParallelRecordFileProcessor {
	public static final int HASH_OBJECT_SIZE_BYTES = 8+4+4+4+48;
	public static PartProcessedRecordFile processRecordFile(RecordFile recordFile) {
		ByteBuffer dataBuf = recordFile.data().rewind();
		int fileVersionNumber = dataBuf.getInt();
		switch (fileVersionNumber) {
			case 2:
				return processRecordFileV2(recordFile, dataBuf);
			case 5:
				return processRecordFileV5(recordFile, dataBuf);
			default:
				System.err.println("Encountered file with unsupported version number of ["+fileVersionNumber+"] - SKIPPING!");
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
		return new PartProcessedRecordFile(recordFile.isLastFile(), startConsensusTime, transactionRows,
				recordFileRow, balanceChanges);
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
		return new PartProcessedRecordFile(recordFile.isLastFile(), startConsensusTime, transactionRows,
				recordFileRow, balanceChanges);
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
	}

	/**
	 * Called for each transaction in the record file
	 *
	 * @param transactionRecordData raw bytes for transaction record protobuf
	 * @param transactionData raw bytes for transaction protobuf
	 * @param transactionRow JSON builder for the transaction row output
	 * @param balanceChanges list of balance changes to add to
	 * @param transactionIndex the index of the transaction within the record file
	 * @return transactions consensus time stamp in epoc nanos
	 */
	public static long handleRecordTransaction(ByteBuffer transactionRecordData, ByteBuffer transactionData,
			JsonObjectBuilder transactionRow, List<BalanceChange> balanceChanges, long transactionIndex) {
		try {
			// parse proto buf messages
			final TransactionRecord transactionRecordMessage =
					TransactionRecord.parseFrom(transactionRecordData);
			final Transaction transactionRoot = Transaction.parseFrom(transactionData);
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
						.add("account_number", Long.toString(amount.getAccountID().getAccountNum()))
						.add("amount", Long.toString(amount.getAmount()))
						.add("is_approval", amount.getIsApproval())
						.build()
				);
				balanceChanges.add( new BalanceChange(
					amount.getAccountID().getAccountNum(),
					BalanceChange.HBAR_TOKEN_TYPE,
					amount.getAmount(),
					consensusTimestampNanosLong
				));
			}
			// scan token transfers list
			final JsonArrayBuilder transfersTokens = Json.createArrayBuilder();
			final JsonArrayBuilder transfersNfts = Json.createArrayBuilder();
			for(TokenTransferList tokenTransferList : transactionRecordMessage.getTokenTransferListsList()) {
				final long tokenEntityNum = tokenTransferList.getToken().getTokenNum();
				for(AccountAmount amount : tokenTransferList.getTransfersList()) {
					transfersTokens.add(Json.createObjectBuilder()
							.add("account", accountIdToString(amount.getAccountID()))
							.add("account_number", Long.toString(amount.getAccountID().getAccountNum()))
							.add("amount", Long.toString(amount.getAmount()))
							.add("token_id", Long.toString(tokenEntityNum))
							.add("is_approval", amount.getIsApproval())
							.build()
					);
					balanceChanges.add(new BalanceChange(
							amount.getAccountID().getAccountNum(),
							tokenEntityNum,
							amount.getAmount(),
							consensusTimestampNanosLong
					));
				}
				for(NftTransfer nftTransfer : tokenTransferList.getNftTransfersList()) {
					transfersNfts.add(Json.createObjectBuilder()
							.add("sender_account", accountIdToString(nftTransfer.getSenderAccountID()))
							.add("sender_account_number", Long.toString(nftTransfer.getSenderAccountID().getAccountNum()))
							.add("receiver_account", accountIdToString(nftTransfer.getReceiverAccountID()))
							.add("receiver_account_number", Long.toString(nftTransfer.getReceiverAccountID().getAccountNum()))
							.add("serial_number", Long.toString(nftTransfer.getSerialNumber()))
							.add("token_id", Long.toString(tokenEntityNum))
							.add("is_approval", nftTransfer.getIsApproval())
							.build()
					);
					balanceChanges.add(new BalanceChange(
							nftTransfer.getSenderAccountID().getAccountNum(),
							tokenEntityNum,
							-1,
							consensusTimestampNanosLong
					));
					balanceChanges.add(new BalanceChange(
							nftTransfer.getReceiverAccountID().getAccountNum(),
							tokenEntityNum,
							-1,
							consensusTimestampNanosLong
					));
				}
			}

			// handle contract results
			final JsonObjectBuilder contractResults = Json.createObjectBuilder();
			final JsonArrayBuilder contractLogs = Json.createArrayBuilder();
			if (transactionRecordMessage.hasContractCreateResult() || transactionRecordMessage.hasContractCallResult()) {
				final ContractFunctionResult contractFunctionResult =
						transactionRecordMessage.hasContractCreateResult() ? transactionRecordMessage.getContractCreateResult() :
								transactionRecordMessage.getContractCallResult();
				contractResults
						.add("contract_id",contractFunctionResult.getContractID().getContractNum())
						.add("call_result",toHex(contractFunctionResult.getContractCallResult().toByteArray()))
						.add("error_message",contractFunctionResult.getErrorMessage())
						.add("bloom",toHex(contractFunctionResult.getBloom().toByteArray()))
						.add("gas_used",Long.toString(contractFunctionResult.getGasUsed()))
						.add("evm_address",toHex(contractFunctionResult.getEvmAddress().toByteArray()))
						.add("gas_limit",Long.toString(contractFunctionResult.getGas()))
						.add("amount",Long.toString(contractFunctionResult.getAmount()))
						.add("function_parameters",toHex(contractFunctionResult.getFunctionParameters().toByteArray()))
						.add("sender_id",Long.toString(contractFunctionResult.getSenderId().getAccountNum()));
				final List<ContractID> createdContractIdsList = contractFunctionResult.getCreatedContractIDsList();
				if (!createdContractIdsList.isEmpty()) {
					var contractIDsArray = Json.createArrayBuilder();
					for(var id: createdContractIdsList) {
						contractIDsArray.add(Long.toString(id.getContractNum()));
					}
					contractResults.add("created_contract_ids",contractIDsArray);
				}

				final List<ContractLoginfo> createdLogsList = contractFunctionResult.getLogInfoList();
				for (int i = 0; i < createdLogsList.size(); i++) {
					final ContractLoginfo log = createdLogsList.get(i);
					JsonArrayBuilder topicArray = Json.createArrayBuilder();
					for(ByteString topic: log.getTopicList()) {
						topicArray.add(toHex(topic.toByteArray()));
					}
					contractLogs.add(Json.createObjectBuilder()
							.add("index",Integer.toString(i))
							.add("contract_id",Long.toString(log.getContractID().getContractNum()))
							.add("data",toHex(log.getData().toByteArray()))
							.add("bloom",toHex(log.getBloom().toByteArray()))
							.add("topics",topicArray.build())
							.build());
				}
			}
			final JsonObject contractResultsObject = contractResults.build();
			final JsonArray contractLogsArray = contractLogs.build();

			// build extra fields that do not need to be searchable into fields sub JSON
			final JsonObjectBuilder fields = Json.createObjectBuilder();
			fields.add("payer_account_id", accountIdToString(transactionRecordMessage.getTransactionID().getAccountID()))
				.add("node", accountIdToString(transactionMessage.getNodeAccountID()))
				.add("valid_start_ns", "")
				.add("valid_duration_seconds", transactionMessage.getTransactionValidDuration().getSeconds())
				.add("initial_balance", "")
				.add("max_fee", transactionMessage.getTransactionFee())
				.add("charged_tx_fee", transactionRecordMessage.getTransactionFee())
				.add("memo", transactionMessage.getMemo())
				.add("transaction_hash", toHex(transactionRecordMessage.getTransactionHash().toByteArray()))
				.add("transaction_bytes", toHex(transactionMessage.toByteArray()))
				.add("parent_consensus_timestamp",
						Long.toString(getEpocNanosAsLong(transactionRecordMessage.getParentConsensusTimestamp())))
				.add("errata", "")
				.add("alias", toHex(transactionRecordMessage.getAlias().toByteArray()))
				.add("ethereum_hash", toHex(transactionRecordMessage.getEthereumHash().toByteArray()));
			// build ids array from set of ids
			JsonArrayBuilder ids = Json.createArrayBuilder();
			idSet.forEach(ids::add);
			// build JSON row
			transactionRow
					.add("entityId", Long.toString(transactionReceiptToEntityNumber(transactionRecordMessage)))
					.add("type", transactionMessage.getDataCase().toString())
					.add("index", transactionIndex)
					.add("result", transactionRecordMessage.getReceipt().getStatus().toString())
					.add("scheduled",Boolean.toString(transactionMessage.getTransactionID().getScheduled()).toLowerCase())
					.add("nonce",Integer.toString(transactionMessage.getTransactionID().getNonce()))
					.add("transaction_id",transactionIdToString(transactionMessage.getTransactionID()))
					.add("fields",fields.build())
					.add("consensus_timestamp",Long.toString(consensusTimestampNanosLong))
					.add("transfers_hbar",transfersHbar.build())
					.add("transfers_tokens",transfersTokens.build())
					.add("transfers_nfts",transfersNfts.build())
					.add("ids",ids.build());
			// only add non-empty contract results
			if (contractResultsObject.size() > 0)transactionRow.add("contract_results",contractResultsObject);
			if (contractLogsArray.size() > 0)transactionRow.add("contract_logs",contractLogsArray);
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

	private static long transactionReceiptToEntityNumber(TransactionRecord transactionRecordMessage) {
		TransactionReceipt receipt = transactionRecordMessage.getReceipt();
		if (receipt.hasAccountID()) {
			return receipt.getAccountID().getAccountNum();
		} else if (receipt.hasFileID()) {
			return receipt.getFileID().getFileNum();
		} else if (receipt.hasContractID()) {
			return receipt.getContractID().getContractNum();
		} else if (receipt.hasTokenID()) {
			return receipt.getTokenID().getTokenNum();
		} else if (receipt.hasTopicID()) {
			return receipt.getTopicID().getTopicNum();
		} else if (receipt.hasScheduleID()) {
			return receipt.getScheduleID().getScheduleNum();
		} else if (transactionRecordMessage.hasContractCreateResult()) { // store contract id in entity id for contract create
			return transactionRecordMessage.getContractCreateResult().getContractID().getContractNum();
		} else if (transactionRecordMessage.hasContractCallResult()) { // store contract id in entity id for contract call
			return transactionRecordMessage.getContractCallResult().getContractID().getContractNum();
		} else {
			return -1;
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