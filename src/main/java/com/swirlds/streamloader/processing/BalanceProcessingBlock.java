package com.swirlds.streamloader.processing;

import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.NftTransfer;
import com.hederahashgraph.api.proto.java.TokenTransferList;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.BalanceChange;
import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.data.BalanceValue;
import com.swirlds.streamloader.data.JsonRow;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import javax.json.Json;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.swirlds.streamloader.StreamDownloaderMain.BALANCES_TOPIC;
import static com.swirlds.streamloader.input.BalancesLoader.INITIAL_BALANCE_FILE_TIMESTAMP;
import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;

public class BalanceProcessingBlock extends PipelineBlock.Sequential<RecordFileBlock, List<JsonRow>> {
	private final ObjectLongHashMap<BalanceKey> balances;
	private boolean initialBalancesHaveBeenProcessed = false;
	public BalanceProcessingBlock(ObjectLongHashMap<BalanceKey> initialBalances, PipelineLifecycle pipelineLifecycle) {
		super("balance-processor", pipelineLifecycle);
		balances = initialBalances;
	}

	@Override
	public List<JsonRow> processDataItem(final RecordFileBlock recordFileBlock) {
		// First we need to process all transaction records and extract the balance changes
		final RecordFile recordFile = recordFileBlock.recordFile();
		final TransactionRecord[] transactionRecords = recordFile.transactionRecords();
		final List<BalanceChange> balanceChanges = new ArrayList<>();
		for (final TransactionRecord transactionRecord : transactionRecords) {
			// extract consensus time stamp
			final long consensusTimestampNanosLong =
					getEpocNanosAsLong(
							transactionRecord.getConsensusTimestamp().getSeconds(),
							transactionRecord.getConsensusTimestamp().getNanos());
			// scan transfers list
			for (AccountAmount amount : transactionRecord.getTransferList().getAccountAmountsList()) {
				balanceChanges.add(new BalanceChange(
						amount.getAccountID().getAccountNum(),
						BalanceChange.HBAR_TOKEN_TYPE,
						amount.getAmount(),
						consensusTimestampNanosLong
				));
			}
			// scan token transfers list
			for (TokenTransferList tokenTransferList : transactionRecord.getTokenTransferListsList()) {
				final long tokenEntityNum = tokenTransferList.getToken().getTokenNum();
				for (AccountAmount amount : tokenTransferList.getTransfersList()) {
					balanceChanges.add(new BalanceChange(
							amount.getAccountID().getAccountNum(),
							tokenEntityNum,
							amount.getAmount(),
							consensusTimestampNanosLong
					));
				}
				for (NftTransfer nftTransfer : tokenTransferList.getNftTransfersList()) {
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
		}
		final List<JsonRow> jsonRows = new ArrayList<>();
		// now check if initial balances have been loaded
		if (!initialBalancesHaveBeenProcessed) {
			balances.forEachKeyValue(
					(balanceKey,tinyBarBalance) -> jsonRows.add(new JsonRow(BALANCES_TOPIC, Json.createObjectBuilder()
						.add("consensus_timestamp",INITIAL_BALANCE_FILE_TIMESTAMP)
						.add("account_id",Long.toString(balanceKey.accountNum()))
						.add("token_id",Long.toString(balanceKey.tokenType()))
						.add("balance",Long.toString(tinyBarBalance))
						.build())));
			initialBalancesHaveBeenProcessed = true;
		}
		// map for all balance changes from this file, we only need the final balance after all transactions in the file
		final Map<BalanceKey, BalanceValue> balancesAfterAllTransactionsInFile = new HashMap<>();
		// walk all balance changes and compute balance at end of file
		for (BalanceChange balanceChange: balanceChanges) {
			final BalanceKey balanceKey = new BalanceKey(balanceChange.accountNum(), balanceChange.tokenTypeEntityNum());
			final long currentBalance = balances.getIfAbsent(balanceKey, 0L);
			final long newBalance = currentBalance + balanceChange.balanceChange();
			balances.put(balanceKey,newBalance);
			balancesAfterAllTransactionsInFile.put(balanceKey,
					new BalanceValue(newBalance, balanceChange.consensusTimeStamp()));
		}
		// create json or balance changes
		for (Map.Entry<BalanceKey, BalanceValue> balanceChange: balancesAfterAllTransactionsInFile.entrySet()) {
			jsonRows.add(new JsonRow(BALANCES_TOPIC , Json.createObjectBuilder()
					.add("consensus_timestamp",balanceChange.getValue().consensusTimeStamp())
					.add("account_id",Long.toString(balanceChange.getKey().accountNum()))
					.add("token_id",Long.toString(balanceChange.getKey().tokenType()))
					.add("balance",Long.toString(balanceChange.getValue().amount()))
					.build()));
		}
		return jsonRows;
	}
}
