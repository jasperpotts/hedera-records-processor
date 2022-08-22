package com.swirlds.streamloader.processing;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.CryptoAllowance;
import com.hederahashgraph.api.proto.java.CryptoApproveAllowanceTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoDeleteAllowanceTransactionBody;
import com.hederahashgraph.api.proto.java.NftAllowance;
import com.hederahashgraph.api.proto.java.NftRemoveAllowance;
import com.hederahashgraph.api.proto.java.SignedTransaction;
import com.hederahashgraph.api.proto.java.TokenAllowance;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.AllowanceChange;
import com.swirlds.streamloader.data.BalanceChange;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.List;

import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;

public class AllowanceProcessingBlock extends PipelineBlock.Sequential<RecordFileBlock, List<GenericRecord>> {
	private static final Schema ALLOWANCE_AVRO_SCHEMA = new Schema.Parser().parse("""
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "allowance",
			 "fields": [
			     {"name": "consensus_timestamp", "type": "long"},
			     {"name": "owner", "type": "long"},
			     {"name": "spender", "type": "long"},
			     {"name": "allowance_type", "type": "string"},
			     {"name": "amount", "type": "long"},
			     {"name": "is_approval", "type": "boolean"},
			     {"name": "token_id", "type": "long"},
			     {"name": "timestamp_start", "type": "long"},
			     {"name": "timestamp_end", "type": "long"},
			     {"name": "payer_account_id", "type": "long"}
			 ]
			}""");
	public AllowanceProcessingBlock(PipelineLifecycle pipelineLifecycle) {
		super("allowance-processor", pipelineLifecycle);
	}

	@Override
	public List<GenericRecord> processDataItem(final RecordFileBlock recordFileBlock) {
		// First we need to process all transaction records and extract the allowance changes
		final RecordFile recordFile = recordFileBlock.recordFile();
		final List<AllowanceChange> allowanceChanges = new ArrayList<>();
        long payerAccountId; // extracted from the transactionRecord.
		for (int i = 0; i < recordFile.transactions().length; i++) {
			final Transaction transaction = recordFile.transactions()[i];
			final TransactionRecord transactionRecord = recordFile.transactionRecords()[i];
            payerAccountId = transactionRecord.getTransactionID().getAccountID().getAccountNum();
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

			// check for granting new allowance(s) ...
			if (transactionBody.hasCryptoApproveAllowance()) {
				CryptoApproveAllowanceTransactionBody allowance = transactionBody.getCryptoApproveAllowance();
				// first check for hbar allowances
				for (int j = 0; j < allowance.getCryptoAllowancesCount(); j++) {
					CryptoAllowance cryptoAllowance = allowance.getCryptoAllowances(j);
					AllowanceChange newAllowance = new AllowanceChange(consensusTimestamp, 
							cryptoAllowance.getOwner().getAccountNum(),
							cryptoAllowance.getSpender().getAccountNum(), "hbar", cryptoAllowance.getAmount(), true,
							BalanceChange.HBAR_TOKEN_TYPE, consensusTimestamp, consensusTimestamp, payerAccountId);
					allowanceChanges.add(newAllowance);
				}
				// next, check for token allowances
				for (int j = 0; j < allowance.getTokenAllowancesCount(); j++) {
					TokenAllowance tokenAllowance = allowance.getTokenAllowances(j);
					AllowanceChange newAllowance = new AllowanceChange(consensusTimestamp, 
							tokenAllowance.getOwner().getAccountNum(),
							tokenAllowance.getSpender().getAccountNum(), "token", tokenAllowance.getAmount(), true,
							tokenAllowance.getTokenId().getTokenNum(), consensusTimestamp, consensusTimestamp,
							payerAccountId);
					allowanceChanges.add(newAllowance);
				}
				// next, check for nft allowances
				for (int j = 0; j < allowance.getNftAllowancesCount(); j++) {
					NftAllowance nftAllowance = allowance.getNftAllowances(j);
					AllowanceChange newAllowance = new AllowanceChange(consensusTimestamp, 
							nftAllowance.getOwner().getAccountNum(),
							nftAllowance.getSpender().getAccountNum(), "nft", 1L, true,
							nftAllowance.getTokenId().getTokenNum(), consensusTimestamp, consensusTimestamp,
							payerAccountId);
					allowanceChanges.add(newAllowance);
				}
			}

			// also check for removing allowance(s) ...
			if (transactionBody.hasCryptoDeleteAllowance()) {
				CryptoDeleteAllowanceTransactionBody allowance = transactionBody.getCryptoDeleteAllowance();
				// no hbar or token remove allowances, only NFTs.
				for (int j = 0; j < allowance.getNftAllowancesCount(); j++) {
					NftRemoveAllowance nftAllowance = allowance.getNftAllowances(j);
					AllowanceChange newAllowance = new AllowanceChange(consensusTimestamp, 
							nftAllowance.getOwner().getAccountNum(),
							nftAllowance.getOwner().getAccountNum(), "nft", 1L, false,
							nftAllowance.getTokenId().getTokenNum(), consensusTimestamp, consensusTimestamp,
							payerAccountId);
					allowanceChanges.add(newAllowance);
				}
			}
		}
		final List<GenericRecord> records = new ArrayList<>();
		// create json for allowance changes
		for (AllowanceChange allowanceChange : allowanceChanges) {
			records.add(new GenericRecordBuilder(ALLOWANCE_AVRO_SCHEMA)
					.set("consensus_timestamp", allowanceChange.consensusTimeStamp())
					.set("owner", allowanceChange.owner())
					.set("spender", allowanceChange.spender())
					.set("allowance_type", allowanceChange.allowanceType())
					.set("amount", allowanceChange.amount())
					.set("is_approval", allowanceChange.isApproval())
					.set("token_id", allowanceChange.tokenId())
					.set("timestamp_start", allowanceChange.timestampStart())
					.set("timestamp_end", allowanceChange.timestampEnd())
					.set("payer_account_id", allowanceChange.payerAccountId())
					.build());
		}
		return records;
	}
}
