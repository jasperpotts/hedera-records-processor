package com.swirlds.streamloader.processing;

import com.google.protobuf.Descriptors;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.json.Json;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RecordFileProcessingBlock extends PipelineBlock.Parallel<RecordFileBlock, List<GenericRecord>> {
	private static final ByteBuffer EMPTY_BYTES = ByteBuffer.allocate(0);
	private static final Schema ARRAY_OF_BYTES_SCHEMA = Schema.createArray(Schema.create(Schema.Type.BYTES));
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
			     {"name": "address_books", "type": { "type": "array", "items" : "string"},"default": []},
			     {"name": "signature_files_data", "type": { "type": "array", "items" : "string"},"default": []},
			     {"name": "signature_files_nodes", "type": { "type": "array", "items" : "int"},"default": []},
			     {"name": "fields", "type": "string"},
			     {"name": "start_running_hash_object", "type": "bytes", "default" : ""},
			     {"name": "end_running_hash_object", "type": "bytes", "default" : ""}
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
				.set("fields", Json.createObjectBuilder()
						.add("count", Long.toString(recordFile.transactions().length))
						.add("gas_used", Long.toString(Arrays.stream(recordFile.transactionRecords()).mapToLong(this::getTransactionGas).sum()))
						.add("hapi_version", recordFile.hapiVersion())
						.add("logs_bloom", "null")
						.add("name", recordFile.fileName())
						.add("size", Long.toString(recordFile.sizeBytes()))
						.build().toString());
		// TODO recordBuilder.set("address_books", new GenericData.Array<ByteBuffer>(0,ARRAY_OF_BYTES_SCHEMA));
		// TODO recordBuilder.set("signature_files_data", new GenericData.Array<ByteBuffer>(0,ARRAY_OF_BYTES_SCHEMA));
		// TODO recordBuilder.set("signature_files_nodes", new GenericData.Array<ByteBuffer>(0,ARRAY_OF_BYTES_SCHEMA));
		if (recordFile.startRunningHash() != null) {
			recordBuilder.set("start_running_hash_object", ByteBuffer.wrap(recordFile.startRunningHash()));
		}
		if (recordFile.endRunningHash() != null) {
			recordBuilder.set("end_running_hash_object", ByteBuffer.wrap(recordFile.endRunningHash()));
		}
		if(recordFile.hashOfPrevFile().isPresent()) {
			recordBuilder.set("prev_hash", ByteBuffer.wrap(recordFile.hashOfPrevFile().get()));
		}
		try {
			return Collections.singletonList(recordBuilder.build());
		} catch( Exception e) {
			e.printStackTrace();
			throw e;
		}
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
