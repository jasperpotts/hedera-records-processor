package com.swirlds.streamloader.processing;

import com.swirlds.streamloader.data.BalanceChange;
import com.swirlds.streamloader.data.BalanceKey;
import com.swirlds.streamloader.data.BalanceValue;
import com.swirlds.streamloader.data.PartProcessedRecordFile;
import com.swirlds.streamloader.data.ProcessedRecordFile;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;

import javax.json.Json;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Does all processing on the records stream that has to be done sequentially and can't be done in parallel. For example
 * calculating balances, as each one depends on the transactions before it.
 */
public class SequentialRecordFileProcessor {
	/**
	 * Given a part processed record file apply all balance deltas and compute new account balances
	 */
	public static ProcessedRecordFile processRecordFile(PartProcessedRecordFile partProcessedRecordFile,
			ObjectLongHashMap<BalanceKey> balances) {
		// map for all balance changes from this file, we only need the final balance after all transactions in the file
		final Map<BalanceKey, BalanceValue> balancesAfterAllTransactionsInFile = new HashMap<>();
		// walk all balance changes and compute balance at end of file
		for (BalanceChange balanceChange: partProcessedRecordFile.balanceChanges()) {
			final BalanceKey balanceKey = new BalanceKey(balanceChange.accountNum(), balanceChange.tokenTypeEntityNum());
			final long currentBalance = balances.getIfAbsent(balanceKey, 0L);
			final long newBalance = currentBalance + balanceChange.balanceChange();
			balances.put(balanceKey,newBalance);
			balancesAfterAllTransactionsInFile.put(balanceKey,
					new BalanceValue(newBalance, balanceChange.consensusTimeStamp()));
		}
		// create json or balance changes
		final List<JsonObject> balanceChangesForFile = new ArrayList<>();
		for (Map.Entry<BalanceKey, BalanceValue> balanceChange: balancesAfterAllTransactionsInFile.entrySet()) {
			balanceChangesForFile.add(Json.createObjectBuilder()
					.add("consensus_timestamp",balanceChange.getValue().consensusTimeStamp())
					.add("account_id",Long.toString(balanceChange.getKey().accountNum()))
					.add("token_id",Long.toString(balanceChange.getKey().tokenType()))
					.add("balance",Long.toString(balanceChange.getValue().amount()))
					.build());
		}
		// create processed file
		return new ProcessedRecordFile(
				partProcessedRecordFile.isLastFile(),
				partProcessedRecordFile.startConsensusTimestamp(),
				partProcessedRecordFile.transactionsRows(),
				partProcessedRecordFile.recordFileRow(),
				balanceChangesForFile);
	}
}
