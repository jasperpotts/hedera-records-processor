package com.swirlds.streamloader.processing;

import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.json.Json;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RecordFileProcessingBlock extends PipelineBlock.Parallel<RecordFileBlock, List<GenericRecord>> {
	private static final Schema RECORD_FILE_AVRO_SCHEMA = new Schema.Parser().parse("""
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "record",
			 "fields": [
			     {"name": "consensus_start_timestamp", "type": "long"},
			     {"name": "consensus_end_timestamp", "type": "long"},
			     {"name": "data_hash", "type": "bytes", "default" : ""},
			     {"name": "prev_hash", "type": "bytes", "default" : ""},
			     {"name": "number", "type": "int"},
			     {"name": "address_books", "type": "string"},
			     {"name": "signature_files", "type": "string"},
			     {"name": "fields", "type": "string"}
			 ]
			}""");
	public RecordFileProcessingBlock(PipelineLifecycle pipelineLifecycle) {
		super("record-file-processor", pipelineLifecycle);
	}

	@Override
	public List<GenericRecord> processDataItem(final RecordFileBlock recordFileBlock) {
		final RecordFile recordFile = recordFileBlock.recordFile();
		final GenericRecordBuilder recordBuilder = new GenericRecordBuilder(RECORD_FILE_AVRO_SCHEMA)
				.set("consensus_start_timestamp", recordFile.startConsensusTime())
				.set("consensus_end_timestamp", recordFile.endConsensusTime())
				.set("data_hash", ByteBuffer.wrap(recordFile.hashOfThisFile()))
				.set("number", recordFileBlock.blockNumber())
				.set("address_books", "[]")
				.set("signature_files", "[]")
				.set("fields", Json.createObjectBuilder()
						.add("count", Long.toString(recordFile.transactions().length))
						.add("gas_used", Long.toString(Arrays.stream(recordFile.transactionRecords()).mapToLong(this::getTransactionGas).sum()))
						.add("hapi_version", recordFile.hapiVersion())
						.add("logs_bloom", "null")
						.add("name", recordFile.fileName())
						.add("size", Long.toString(recordFile.sizeBytes()))
						.build().toString());

		if(recordFile.hashOfPrevFile().isPresent()) {
			recordBuilder.set("prev_hash", ByteBuffer.wrap(recordFile.hashOfPrevFile().get()));
		}
		return Collections.singletonList(recordBuilder.build());
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
