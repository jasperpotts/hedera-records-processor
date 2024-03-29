package com.swirlds.streamloader.processing;

import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.NftTransfer;
import com.hederahashgraph.api.proto.java.TokenTransferList;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.BalanceChange;
import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.data.BalanceValue;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.swirlds.streamloader.input.BalancesLoader.INITIAL_BALANCE_FILE_TIMESTAMP_LONG;
import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;

public class BalanceProcessingBlock extends PipelineBlock.Sequential<RecordFileBlock, List<GenericRecord>> {
	private static final Schema BALANCES_AVRO_SCHEMA = new Schema.Parser().parse("""
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "balance",
			 "fields": [
			     {"name": "consensus_timestamp", "type": "long"},
			     {"name": "account_id", "type": "long"},
			     {"name": "token_id", "type": "long"},
			     {"name": "balance", "type": "long"}
			 ]
			}""");
	private final ObjectLongHashMap<BalanceKey> balances;
	private boolean initialBalancesHaveBeenProcessed = false;
	public BalanceProcessingBlock(ObjectLongHashMap<BalanceKey> initialBalances, PipelineLifecycle pipelineLifecycle) {
		super("balance-processor", pipelineLifecycle);
		balances = initialBalances;
	}


	@Override
	public List<GenericRecord> processDataItem(final RecordFileBlock recordFileBlock) {
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
							1,
							consensusTimestampNanosLong
					));
				}
			}
		}
		final List<GenericRecord> records = new ArrayList<>();
		// now check if initial balances have been loaded
		if (!initialBalancesHaveBeenProcessed) {
			balances.forEachKeyValue(
					(balanceKey,tinyBarBalance) -> records.add(new GenericRecordBuilder(BALANCES_AVRO_SCHEMA)
						.set("consensus_timestamp",INITIAL_BALANCE_FILE_TIMESTAMP_LONG)
						.set("account_id",balanceKey.accountNum())
						.set("token_id",balanceKey.tokenType())
						.set("balance",tinyBarBalance)
						.build()));
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
			records.add(new GenericRecordBuilder(BALANCES_AVRO_SCHEMA)
					.set("consensus_timestamp",balanceChange.getValue().consensusTimeStamp())
					.set("account_id",balanceChange.getKey().accountNum())
					.set("token_id",balanceChange.getKey().tokenType())
					.set("balance",balanceChange.getValue().amount())
					.build());
		}
		return records;
	}
}
