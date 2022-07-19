package com.swirlds.streamloader.processing;

import com.swirlds.streamloader.data.PartProcessedRecordFile;
import com.swirlds.streamloader.data.ProcessedRecordFile;

import java.util.Collections;

public class BalanceProcessor {
	/**
	 * Given a part processed record file apply all balance deltas and compute new account balances
	 */
	public static ProcessedRecordFile processBalances(PartProcessedRecordFile partProcessedRecordFile) {
		return new ProcessedRecordFile(
				partProcessedRecordFile.transactionsRows(),
				partProcessedRecordFile.recordFileRow(),
				Collections.emptyList());
	}
}
