package com.swirlds.streamloader.processing;

import com.google.protobuf.ByteString;
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
import com.swirlds.streamloader.data.JsonRow;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static com.swirlds.streamloader.StreamDownloaderMain.TRANSACTIONS_TOPIC;
import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;
import static com.swirlds.streamloader.util.Utils.toHex;

public class TransactionProcessingBlock extends PipelineBlock.Parallel<RecordFileBlock, List<JsonRow>> {
	public TransactionProcessingBlock(PipelineLifecycle pipelineLifecycle) {
		super("transaction-processor", pipelineLifecycle);
	}

	@Override
	public List<JsonRow> processDataItem(final RecordFileBlock recordFileBlock) throws Exception {
		final RecordFile recordFile = recordFileBlock.recordFile();
		final Transaction[] transactions = recordFile.transactions();
		final TransactionRecord[] transactionRecords = recordFile.transactionRecords();
		final List<JsonRow> jsonRows = new ArrayList<>(transactions.length);
		for (int t = 0; t < transactions.length; t++) {
			final TransactionRecord transactionRecord = transactionRecords[t];
			final Transaction transaction = transactions[t];
			// handle the 3 ways that transaction body has been stored in file over time
			final TransactionBody transactionMessage;
			if (transaction.hasBody()) {
				transactionMessage = transaction.getBody();
			} else {
				ByteString bodyBytes = transaction.getBodyBytes();
				if (bodyBytes.isEmpty()) {
					final var signedTransactionBytes = transaction.getSignedTransactionBytes();
					bodyBytes = SignedTransaction.parseFrom(signedTransactionBytes).getBodyBytes();
				}
				transactionMessage = TransactionBody.parseFrom(bodyBytes);
			}
			// extract consensus time stamp
			final long consensusTimestampNanosLong =
					getEpocNanosAsLong(
							transactionRecord.getConsensusTimestamp().getSeconds(),
							transactionRecord.getConsensusTimestamp().getNanos());
			// scan transfers list
			final JsonArrayBuilder transfersHbar = Json.createArrayBuilder();
			final TreeSet<Long> idSet = new TreeSet<>();
			for (AccountAmount amount : transactionRecord.getTransferList().getAccountAmountsList()) {
				idSet.add(amount.getAccountID().getAccountNum());
				transfersHbar.add(Json.createObjectBuilder()
						.add("account", accountIdToString(amount.getAccountID()))
						.add("account_number", Long.toString(amount.getAccountID().getAccountNum()))
						.add("amount", Long.toString(amount.getAmount()))
						.add("is_approval", amount.getIsApproval())
						.build()
				);
			}
			// scan token transfers list
			final JsonArrayBuilder transfersTokens = Json.createArrayBuilder();
			final JsonArrayBuilder transfersNfts = Json.createArrayBuilder();
			for (TokenTransferList tokenTransferList : transactionRecord.getTokenTransferListsList()) {
				final long tokenEntityNum = tokenTransferList.getToken().getTokenNum();
				for (AccountAmount amount : tokenTransferList.getTransfersList()) {
					transfersTokens.add(Json.createObjectBuilder()
							.add("account", accountIdToString(amount.getAccountID()))
							.add("account_number", Long.toString(amount.getAccountID().getAccountNum()))
							.add("amount", Long.toString(amount.getAmount()))
							.add("token_id", Long.toString(tokenEntityNum))
							.add("is_approval", amount.getIsApproval())
							.build()
					);
				}
				for (NftTransfer nftTransfer : tokenTransferList.getNftTransfersList()) {
					transfersNfts.add(Json.createObjectBuilder()
							.add("sender_account", accountIdToString(nftTransfer.getSenderAccountID()))
							.add("sender_account_number",
									Long.toString(nftTransfer.getSenderAccountID().getAccountNum()))
							.add("receiver_account", accountIdToString(nftTransfer.getReceiverAccountID()))
							.add("receiver_account_number",
									Long.toString(nftTransfer.getReceiverAccountID().getAccountNum()))
							.add("serial_number", Long.toString(nftTransfer.getSerialNumber()))
							.add("token_id", Long.toString(tokenEntityNum))
							.add("is_approval", nftTransfer.getIsApproval())
							.build()
					);
				}
			}

			// handle contract results
			final JsonObjectBuilder contractResults = Json.createObjectBuilder();
			final JsonArrayBuilder contractLogs = Json.createArrayBuilder();
			if (transactionRecord.hasContractCreateResult() || transactionRecord.hasContractCallResult()) {
				final ContractFunctionResult contractFunctionResult =
						transactionRecord.hasContractCreateResult() ? transactionRecord.getContractCreateResult() :
								transactionRecord.getContractCallResult();
				contractResults
						.add("contract_id", contractFunctionResult.getContractID().getContractNum())
						.add("call_result", toHex(contractFunctionResult.getContractCallResult().toByteArray()))
						.add("error_message", contractFunctionResult.getErrorMessage())
						.add("bloom", toHex(contractFunctionResult.getBloom().toByteArray()))
						.add("gas_used", Long.toString(contractFunctionResult.getGasUsed()))
						.add("evm_address", toHex(contractFunctionResult.getEvmAddress().toByteArray()))
						.add("gas_limit", Long.toString(contractFunctionResult.getGas()))
						.add("amount", Long.toString(contractFunctionResult.getAmount()))
						.add("function_parameters", toHex(contractFunctionResult.getFunctionParameters().toByteArray()))
						.add("sender_id", Long.toString(contractFunctionResult.getSenderId().getAccountNum()));
				final List<ContractID> createdContractIdsList = contractFunctionResult.getCreatedContractIDsList();
				if (!createdContractIdsList.isEmpty()) {
					var contractIDsArray = Json.createArrayBuilder();
					for (var id : createdContractIdsList) {
						contractIDsArray.add(Long.toString(id.getContractNum()));
					}
					contractResults.add("created_contract_ids", contractIDsArray);
				}

				final List<ContractLoginfo> createdLogsList = contractFunctionResult.getLogInfoList();
				for (int i = 0; i < createdLogsList.size(); i++) {
					final ContractLoginfo log = createdLogsList.get(i);
					JsonArrayBuilder topicArray = Json.createArrayBuilder();
					for (ByteString topic : log.getTopicList()) {
						topicArray.add(toHex(topic.toByteArray()));
					}
					contractLogs.add(Json.createObjectBuilder()
							.add("index", Integer.toString(i))
							.add("contract_id", Long.toString(log.getContractID().getContractNum()))
							.add("data", toHex(log.getData().toByteArray()))
							.add("bloom", toHex(log.getBloom().toByteArray()))
							.add("topics", topicArray.build())
							.build());
				}
			}
			final JsonObject contractResultsObject = contractResults.build();
			final JsonArray contractLogsArray = contractLogs.build();

			// build extra fields that do not need to be searchable into fields sub JSON
			final JsonObjectBuilder fields = Json.createObjectBuilder();
			fields.add("payer_account_id", accountIdToString(transactionRecord.getTransactionID().getAccountID()))
					.add("node", accountIdToString(transactionMessage.getNodeAccountID()))
					.add("valid_start_ns", "")
					.add("valid_duration_seconds", transactionMessage.getTransactionValidDuration().getSeconds())
					.add("initial_balance", "")
					.add("max_fee", transactionMessage.getTransactionFee())
					.add("charged_tx_fee", transactionRecord.getTransactionFee())
					.add("memo", transactionMessage.getMemo())
					.add("transaction_hash", toHex(transactionRecord.getTransactionHash().toByteArray()))
					.add("transaction_bytes", toHex(transactionMessage.toByteArray()))
					.add("parent_consensus_timestamp",
							Long.toString(getEpocNanosAsLong(transactionRecord.getParentConsensusTimestamp())))
					.add("errata", "")
					.add("alias", toHex(transactionRecord.getAlias().toByteArray()))
					.add("ethereum_hash", toHex(transactionRecord.getEthereumHash().toByteArray()));
			// build ids array from set of ids
			JsonArrayBuilder ids = Json.createArrayBuilder();
			idSet.forEach(ids::add);
			// build JSON row
			final var transactionRow = Json.createObjectBuilder()
					.add("entityId", Long.toString(transactionReceiptToEntityNumber(transactionRecord)))
					.add("type", transactionMessage.getDataCase().toString())
					.add("index", t)
					.add("result", transactionRecord.getReceipt().getStatus().toString())
					.add("scheduled",
							Boolean.toString(transactionMessage.getTransactionID().getScheduled()).toLowerCase())
					.add("nonce", Integer.toString(transactionMessage.getTransactionID().getNonce()))
					.add("transaction_id", transactionIdToString(transactionMessage.getTransactionID()))
					.add("fields", fields.build())
					.add("consensus_timestamp", Long.toString(consensusTimestampNanosLong))
					.add("transfers_hbar", transfersHbar.build())
					.add("transfers_tokens", transfersTokens.build())
					.add("transfers_nfts", transfersNfts.build())
					.add("ids", ids.build());
			// only add non-empty contract results
			if (contractResultsObject.size() > 0) transactionRow.add("contract_results", contractResultsObject);
			if (contractLogsArray.size() > 0) transactionRow.add("contract_logs", contractLogsArray);
			// create new row
			jsonRows.add(new JsonRow(TRANSACTIONS_TOPIC, transactionRow.build()));
		}
		return jsonRows;
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
