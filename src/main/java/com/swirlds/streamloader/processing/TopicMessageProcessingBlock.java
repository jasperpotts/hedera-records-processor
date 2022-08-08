package com.swirlds.streamloader.processing;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.ConsensusMessageChunkInfo;
import com.hederahashgraph.api.proto.java.ConsensusSubmitMessageTransactionBody;
import com.hederahashgraph.api.proto.java.SignedTransaction;
import com.hederahashgraph.api.proto.java.TopicID;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionReceipt;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.data.TopicMessageChange;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;
import static com.swirlds.streamloader.util.Utils.toHex;

public class TopicMessageProcessingBlock extends PipelineBlock.Sequential<RecordFileBlock, List<GenericRecord>> {
	private static final Schema TOPIC_KESSAGE_AVRO_SCHEMA = new Schema.Parser().parse("""
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "topic_message",
			 "fields": [
			     {"name": "consensus_timestamp", "type": "long"},
			     {"name": "message", "type": "bytes", "default" : ""},
			     {"name": "running_hash", "type": "bytes", "default" : ""},
			     {"name": "sequence_number", "type": "int"},
			     {"name": "running_hash_version", "type": "int"},
			     {"name": "chunk_number", "type": "int"},
			     {"name": "chunk_total", "type": "int"},
			     {"name": "pauer_account_id", "type": "long"},
			     {"name": "topic_id", "type": "int"},
			     {"name": "initial_transaction_id", "type": "string"},
			     {"name": "noncw", "type": "int"},
			     {"name": "scheduled", "type": "boolean"},
			     {"name": "consensus_start_timestamp", "type": "long"}
			}""");

	public TopicMessageProcessingBlock(PipelineLifecycle pipelineLifecycle) {
		super("topic-message-processor", pipelineLifecycle);
	}

	@Override
	public List<GenericRecord> processDataItem(final RecordFileBlock recordFileBlock) {
		// First we need to process all transaction records and extract the topic message changes
		final RecordFile recordFile = recordFileBlock.recordFile();
		final List<TopicMessageChange> topicMessageChanges = new ArrayList<>();
		for (int i = 0; i < recordFile.transactions().length; i++) {
			final Transaction transaction = recordFile.transactions()[i];
			final TransactionRecord transactionRecord = recordFile.transactionRecords()[i];
			// extract consensus time stamp
			final long consensusTimestamp = getEpocNanosAsLong(
					transactionRecord.getConsensusTimestamp().getSeconds(),
					transactionRecord.getConsensusTimestamp().getNanos());
			final TransactionBody transactionBody;
			if (transaction.hasBody()) {
				transactionBody = transaction.getBody();
			} else {
				ByteString bodyBytes = transaction.getBodyBytes();
				if (bodyBytes.isEmpty()) {
					final var signedTransactionBytes = transaction.getSignedTransactionBytes();
					try {
						bodyBytes = SignedTransaction.parseFrom(signedTransactionBytes).getBodyBytes();
					} catch (InvalidProtocolBufferException e) {
						// no log to warn on?
						// leave bodyBytes as they were -- this will throw again in 4 lines
					}
				}
				try {
					transactionBody = TransactionBody.parseFrom(bodyBytes);
				} catch (InvalidProtocolBufferException e) {
					// skip this transaction, since we can't extract a valid transactionBody
					continue;
				}
			}

			// check for presence of topic messages
			if (transactionBody.hasConsensusSubmitMessage()) {
				ConsensusSubmitMessageTransactionBody message = transactionBody.getConsensusSubmitMessage();
				TransactionReceipt receipt = transactionRecord.getReceipt();
				int topicId = (int) message.getTopicID().getTopicNum();
				int runningHashVersion = receipt.getTopicRunningHashVersion() == 0 ? 1 : (int) receipt
						.getTopicRunningHashVersion();
				// defaults when there is no fragmented topic message present
				int chunkNumber = 0;
				int chunkTotal = 0;
				String initialTransactionId = "";
				// Handle optional fragmented topic message
				if (message.hasChunkInfo()) {
					ConsensusMessageChunkInfo chunkInfo = message.getChunkInfo();
					chunkNumber = chunkInfo.getNumber();
					chunkTotal = chunkInfo.getTotal();

					if (chunkInfo.hasInitialTransactionID()) {
						initialTransactionId = toHex(chunkInfo.getInitialTransactionID().toByteArray());
					}
				}
				byte[] messageBytes = message.getMessage().toByteArray();
				byte[] runningHashBytes = receipt.getTopicRunningHash().toByteArray();
				int sequenceNumber = (int) receipt.getTopicSequenceNumber();
				long payerAccountId = transactionRecord.getTransactionID().getAccountID().getAccountNum();
				int nonce = transactionBody.getTransactionID().getNonce();
				boolean scheduled = transactionBody.getTransactionID().getScheduled();
				long consensusStartTimeStamp = recordFile.startConsensusTime();

				TopicMessageChange newTopicMessage = new TopicMessageChange(consensusTimestamp, messageBytes,
						runningHashBytes, sequenceNumber, runningHashVersion, chunkNumber, chunkTotal, payerAccountId,
						topicId, initialTransactionId, nonce, scheduled, consensusStartTimeStamp);
				topicMessageChanges.add(newTopicMessage);
			}
		}

		final List<GenericRecord> records = new ArrayList<>();
		// create json for topic message changes
		for (TopicMessageChange topicMessageChange : topicMessageChanges) {
			records.add(new GenericRecordBuilder(TOPIC_KESSAGE_AVRO_SCHEMA)
					.set("consensus_timestamp", topicMessageChange.consensusTimeStamp())
					.set("message", topicMessageChange.message())
					.set("running_hash", topicMessageChange.runningHash())
					.set("sequence_number", topicMessageChange.sequenceNumber())
					.set("running_hash_version", topicMessageChange.runningHashVersion())
					.set("chunk_number", topicMessageChange.chunkNumber())
					.set("chunk_total", topicMessageChange.chunkTotal())
					.set("payer_account_id", topicMessageChange.payerAccountId())
					.set("topic_id", topicMessageChange.topicId())
					.set("initial_transaction_id", topicMessageChange.initialTransactionId())
					.set("nonce", topicMessageChange.nonce())
					.set("scheduled", topicMessageChange.scheduled())
					.set("consensus_start_timestamp", topicMessageChange.consensusStartTimeStamp())
					.build());
		}
		return records;
	}
}
