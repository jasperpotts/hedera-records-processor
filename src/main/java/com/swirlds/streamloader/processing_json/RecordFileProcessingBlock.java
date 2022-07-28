package com.swirlds.streamloader.processing_json;

import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.JsonRow;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import com.swirlds.streamloader.util.Utils;

import javax.json.Json;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.swirlds.streamloader.StreamDownloaderMain.RECORDS_TOPIC;

public class RecordFileProcessingBlock extends PipelineBlock.Parallel<RecordFileBlock, List<JsonRow>> {
	public RecordFileProcessingBlock(PipelineLifecycle pipelineLifecycle) {
		super("record-file-processor", pipelineLifecycle);
	}

	@Override
	public List<JsonRow> processDataItem(final RecordFileBlock recordFileBlock) {
		final RecordFile recordFile = recordFileBlock.recordFile();
		return Collections.singletonList(new JsonRow(RECORDS_TOPIC,Json.createObjectBuilder()
				.add("consensus_start_timestamp", Long.toString(recordFile.startConsensusTime()))
				.add("consensus_end_timestamp", Long.toString(recordFile.endConsensusTime()))
				.add("data_hash", Utils.toHex(recordFile.hashOfThisFile()))
				.add("prev_hash", Utils.toHex(recordFile.hashOfPrevFile().orElseGet(() -> new byte[0])))
				.add("number", Long.toString(recordFileBlock.blockNumber()))
				.add("address_books", Json.createArrayBuilder().build())
				.add("signature_files", Json.createArrayBuilder().build())
				.add("fields", Json.createObjectBuilder()
						.add("count", Long.toString(recordFile.transactions().length))
						.add("gas_used", Long.toString(Arrays.stream(recordFile.transactionRecords()).mapToLong(this::getTransactionGas).sum()))
						.add("hapi_version", recordFile.hapiVersion())
						.add("logs_bloom", "null")
						.add("name", recordFile.fileName())
						.add("size", Long.toString(recordFile.sizeBytes()))
						.build())
				.build()));
	}

	private long getTransactionGas(TransactionRecord transactionRecord) {
		if (transactionRecord.hasContractCallResult()) {
			return transactionRecord.getContractCallResult().getGasUsed();
		} else if (transactionRecord.hasContractCreateResult()) {
			return transactionRecord.getContractCreateResult().getGasUsed();
		} else {
			return 0;
		}
	}
}
