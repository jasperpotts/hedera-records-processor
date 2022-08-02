package com.swirlds.streamloader.processing;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.AccountAmount;
import com.hederahashgraph.api.proto.java.NftTransfer;
import com.hederahashgraph.api.proto.java.SignedTransaction;
import com.hederahashgraph.api.proto.java.TokenTransferList;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.NftChange;
import com.swirlds.streamloader.data.NftKey;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;

public class NftProcessingBlock extends PipelineBlock.Sequential<RecordFileBlock, List<GenericRecord>> {
	private static final Schema NFT_AVRO_SCHEMA = new Schema.Parser().parse("""
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "nft",
			 "fields": [
			     {"name": "consensus_timestamp", "type": "long"},
			     {"name": "account_id", "type": "long"},
			     {"name": "deleted", "type": "boolean"},
			     {"name": "metadata_1", "type": "string", "default": ""},
			     {"name": "serialNumber", "type": "long"},
			     {"name": "token_id", "type": "long"},
			     {"name": "delegating_spender", "type": "long"},
			     {"name": "spender", "type": "long"}
			 ]
			}""");
	private final HashMap<NftKey, String> nfts; // map from <serialNumber, tokenId> -> Metadata
	public NftProcessingBlock(HashMap<NftKey, String> initialNfts, PipelineLifecycle pipelineLifecycle) {
		super("nft-processor", pipelineLifecycle);
		nfts = initialNfts;
	}

	@Override
	public List<GenericRecord> processDataItem(final RecordFileBlock recordFileBlock) {
		// First we need to process all transaction records and extract the nft changes
		final RecordFile recordFile = recordFileBlock.recordFile();
		final List<NftChange> nftChanges = new ArrayList<>();
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
			// is it an NFT mint action?
			if (transactionBody.hasTokenMint()) {
				var tokenMint = transactionBody.getTokenMint();
				long tokenId = tokenMint.getToken().getTokenNum(); // assume shard & realm == 0
				List<Long> serialNumbers = transactionRecord.getReceipt().getSerialNumbersList();
				for (int j = 0; j < serialNumbers.size(); j++) {
					NftChange nft = new NftChange(consensusTimestamp, consensusTimestamp,
							transactionRecord.getReceipt().getAccountID().getAccountNum(), false,
                			tokenMint.getMetadata(j).toStringUtf8(),
                			transactionRecord.getReceipt().getSerialNumbers(j), tokenId, 0, 0);
					nftChanges.add(nft);
				}
			}

			// is it an NFT burn action?
			if (transactionBody.hasTokenBurn()) {
				var tokenBurn = transactionBody.getTokenBurn();
				long tokenId = tokenBurn.getToken().getTokenNum(); // assume shard & realm == 0
				List<Long> serialNumbers = tokenBurn.getSerialNumbersList();
				for (int j = 0; j < serialNumbers.size(); j++) {
					long serialNumber = tokenBurn.getSerialNumbers(j);
					NftKey key = new NftKey(serialNumber, tokenId);
					NftChange nft = new NftChange(consensusTimestamp, consensusTimestamp,
							transactionRecord.getReceipt().getAccountID().getAccountNum(), true,
							nfts.get(key), serialNumber, tokenId, 0, 0);
					nftChanges.add(nft);
				}
			}

			// is it an NFT transfer action?
			if (transactionBody.hasCryptoTransfer()) {
//MYK        		List<TokenTransferList> tokenTransfersLists = transactionBody.getCryptoTransfer().getTokenTransfersList();
				for (TokenTransferList tokenTransferList : transactionRecord.getTokenTransferListsList()) {
					long tokenId = tokenTransferList.getToken().getTokenNum(); // assume shard & realm == 0
					for (var nftTransfer : tokenTransferList.getNftTransfersList()) {
						long serialNumber = nftTransfer.getSerialNumber();
						NftKey key = new NftKey(serialNumber, tokenId);
						NftChange nft = new NftChange(consensusTimestamp, consensusTimestamp,
								transactionRecord.getReceipt().getAccountID().getAccountNum(), false,
								// MYK: check order of delagateSpender vs spender
								nfts.get(key), serialNumber, tokenId, nftTransfer.getSenderAccountID().getAccountNum(),
								nftTransfer.getReceiverAccountID().getAccountNum());
						nftChanges.add(nft);
					}
				}
			}
		}
		final List<GenericRecord> records = new ArrayList<>();
		// create json for nft changes
		for (NftChange nftChange : nftChanges) {
			records.add(new GenericRecordBuilder(NFT_AVRO_SCHEMA)
					.set("consensus_timestamp", nftChange.consensusTimeStamp())
					.set("account_num", nftChange.accountNum())
					.set("deleted", nftChange.deleted())
					.set("metadata", nftChange.metadata())
					.set("serial_number", nftChange.serialNumber())
					.set("token_id", nftChange.tokenId())
					.set("delegating_spender", nftChange.delegatingSpender())
					.set("spender", nftChange.spender())
					.build());
		}
		return records;
	}
}
