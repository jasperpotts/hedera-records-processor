package com.swirlds.streamloader.data;


import javax.json.JsonObject;
import java.util.List;

/**
 * Records all the data out of a single record file, in format ready for output
 *
 * @param transactionsRows
 * @param recordFileRow
 * @param balanceChanges
 */
public record PartProcessedRecordFile(
	boolean isLastFile,
	long startConsensusTimestamp,
	List<JsonObject> transactionsRows,
	JsonObject recordFileRow,
	List<BalanceChange> balanceChanges
) {

}
