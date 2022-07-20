package com.swirlds.streamloader.data;


import javax.json.JsonObject;
import java.util.List;

/**
 * Records all the data out of a single record file, in format ready for output
 *
 * @param transactionsRows
 * @param recordFileRow
 * @param accountBalanceRows
 */
public record ProcessedRecordFile(
	boolean isLastFile,
	long startConsensusTimestamp,
	List<JsonObject> transactionsRows,
	JsonObject recordFileRow,
	List<JsonObject> accountBalanceRows
) {

}
