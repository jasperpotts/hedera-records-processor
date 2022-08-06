package com.swirlds.streamloader.processing;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hederahashgraph.api.proto.java.AccountID;
import com.hederahashgraph.api.proto.java.ConsensusCreateTopicTransactionBody;
import com.hederahashgraph.api.proto.java.ConsensusUpdateTopicTransactionBody;
import com.hederahashgraph.api.proto.java.ContractCreateTransactionBody;
import com.hederahashgraph.api.proto.java.ContractUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoCreateTransactionBody;
import com.hederahashgraph.api.proto.java.CryptoUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.FileAppendTransactionBody;
import com.hederahashgraph.api.proto.java.FileCreateTransactionBody;
import com.hederahashgraph.api.proto.java.FileUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.Key;
import com.hederahashgraph.api.proto.java.KeyList;
import com.hederahashgraph.api.proto.java.SignedTransaction;
import com.hederahashgraph.api.proto.java.Timestamp;
import com.hederahashgraph.api.proto.java.TokenCreateTransactionBody;
import com.hederahashgraph.api.proto.java.TokenUpdateTransactionBody;
import com.hederahashgraph.api.proto.java.Transaction;
import com.hederahashgraph.api.proto.java.TransactionBody;
import com.hederahashgraph.api.proto.java.TransactionRecord;
import com.swirlds.streamloader.data.Entity;
import com.swirlds.streamloader.data.RecordFile;
import com.swirlds.streamloader.data.RecordFileBlock;
import com.swirlds.streamloader.util.PipelineBlock;
import com.swirlds.streamloader.util.PipelineLifecycle;
import org.apache.avro.generic.GenericRecord;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.jetbrains.annotations.NotNull;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import java.util.ArrayList;
import java.util.List;

import static com.swirlds.streamloader.util.Utils.getEpocNanosAsLong;

@SuppressWarnings("deprecation")
public class EntityProcessingBlock extends PipelineBlock.Sequential<RecordFileBlock, List<GenericRecord>> {
	private final LongObjectHashMap<Entity> entities;

	public EntityProcessingBlock(PipelineLifecycle pipelineLifecycle) {
		super("entity-processor", pipelineLifecycle);
		entities = new LongObjectHashMap<>();
	}

	@Override
	public List<GenericRecord> processDataItem(final RecordFileBlock recordFileBlock) {
		// First we need to process all transaction records and extract the balance changes
		final RecordFile recordFile = recordFileBlock.recordFile();
		final Transaction[] transactions = recordFile.transactions();
		final TransactionRecord[] transactionRecords = recordFile.transactionRecords();
		final List<GenericRecord> records = new ArrayList<>(transactions.length);
		for (int t = 0; t < transactions.length; t++) {
			try {
				final TransactionRecord transactionRecord = transactionRecords[t];
				final Transaction transaction = transactions[t];
				// handle the 3 ways that transaction body has been stored in file over time
				final TransactionBody transactionMessage;
				if (transaction.hasBody()) {
					transactionMessage = transaction.getBody();
				} else {
					ByteString bodyBytes = transaction.getBodyBytes();
					if (bodyBytes.isEmpty()) {
						final var signedTransactionBytes = transaction.getSignedTransactionBytes();
						bodyBytes = SignedTransaction.parseFrom(signedTransactionBytes).getBodyBytes();
					}
					transactionMessage = TransactionBody.parseFrom(bodyBytes);
				}
				// extract consensus time stamp
				final long consensusTimestampNanosLong = getEpocNanosAsLong(
								transactionRecord.getConsensusTimestamp().getSeconds(),
								transactionRecord.getConsensusTimestamp().getNanos());

				if (transactionMessage.hasCryptoCreateAccount()) { // account creation -------------
					Entity entity = accountToEntity(transactionRecord, transactionMessage, consensusTimestampNanosLong);
					entities.put(transactionRecord.getReceipt().getAccountID().getAccountNum(), entity);
					records.add(entity.asAvro());
				} else if (transactionMessage.hasCryptoUpdateAccount()) { // account update -------------
					final var id = transactionMessage.getCryptoUpdateAccount().getAccountIDToUpdate().getAccountNum();
					final Entity entity = entities.getIfAbsentPut(id, new Entity(id, Entity.Type.account));
					entity.setConsensusTimestamp(consensusTimestampNanosLong);
					updateAccount(transactionMessage, entity);
					records.add(entity.asAvro());
				} else if (transactionMessage.hasCryptoDelete()) { // account delete -------------
					final var id = transactionMessage.getCryptoDelete().getDeleteAccountID().getAccountNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						// TODO handle extra data from transactionMessage.getCryptoDelete();
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						entity.getFields().add("deleted",false);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to delete account 0.0."+id+" but had not been created");
					}
				} else if(transactionMessage.hasTokenCreation()) { // create token  -------------
					Entity entity = tokenToEntity(transactionRecord, transactionMessage, consensusTimestampNanosLong);
					entities.put(transactionRecord.getReceipt().getTokenID().getTokenNum(), entity);
					records.add(entity.asAvro());
				} else if (transactionMessage.hasTokenUpdate()) { // token update -------------
					final var id = transactionMessage.getTokenUpdate().getToken().getTokenNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						updateToken(transactionMessage, entity);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to update token 0.0."+id+" but had not been created");
					}
				} else if (transactionMessage.hasTokenDeletion()) { // token delete -------------
					final var id = transactionMessage.getTokenDeletion().getToken().getTokenNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						// TODO handle extra data from transactionMessage.getTokenDeletion();
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						entity.getFields().add("deleted",false);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to delete token 0.0."+id+" but had not been created");
					}
				} else if(transactionMessage.hasConsensusCreateTopic()) { // create topic  -------------
					Entity entity = topicToEntity(transactionRecord, transactionMessage, consensusTimestampNanosLong);
					entities.put(transactionRecord.getReceipt().getTopicID().getTopicNum(), entity);
					records.add(entity.asAvro());
				} else if (transactionMessage.hasConsensusUpdateTopic()) { // topic update -------------
					final var id = transactionMessage.getConsensusUpdateTopic().getTopicID().getTopicNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						updateTopic(transactionMessage, entity);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to update topic 0.0."+id+" but had not been created");
					}
				} else if (transactionMessage.hasConsensusDeleteTopic()) { // topic delete -------------
					final var id = transactionMessage.getConsensusDeleteTopic().getTopicID().getTopicNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						// TODO handle extra data from transactionMessage.getConsensusDeleteTopic();
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						entity.getFields().add("deleted",false);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to delete topic 0.0."+id+" but had not been created");
					}
				} else if(transactionMessage.hasContractCreateInstance()) { // create contract  -------------
					Entity entity = contractToEntity(transactionRecord, transactionMessage, consensusTimestampNanosLong);
					entities.put(transactionRecord.getReceipt().getContractID().getContractNum(), entity);
					records.add(entity.asAvro());
				} else if (transactionMessage.hasContractUpdateInstance()) { // contract update -------------
					final var id = transactionMessage.getContractUpdateInstance().getContractID().getContractNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						updateContract(transactionMessage, entity);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to update contract 0.0."+id+" but had not been created");
					}
				} else if (transactionMessage.hasContractDeleteInstance()) { // contract delete -------------
					final var id = transactionMessage.getContractDeleteInstance().getContractID().getContractNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						// TODO handle extra data from transactionMessage.getContractDeleteInstance();
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						entity.getFields().add("deleted",false);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to delete contract 0.0."+id+" but had not been created");
					}
				} else if(transactionMessage.hasFileCreate()) { // create file  -------------
					Entity entity = fileToEntity(transactionRecord, transactionMessage, consensusTimestampNanosLong);
					entities.put(transactionRecord.getReceipt().getFileID().getFileNum(), entity);
					records.add(entity.asAvro());
				} else if (transactionMessage.hasFileAppend()) { // file append -------------
					final var id = transactionMessage.getFileAppend().getFileID().getFileNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						appendFile(transactionMessage, entity);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to append to file 0.0."+id+" but had not been created");
					}
				} else if (transactionMessage.hasFileUpdate()) { // file update -------------
					final var id = transactionMessage.getFileUpdate().getFileID().getFileNum();
					final Entity entity = entities.getIfAbsentPut(id, new Entity(id, Entity.Type.file));
					entity.setConsensusTimestamp(consensusTimestampNanosLong);
					updateFile(transactionMessage, entity);
					records.add(entity.asAvro());
				} else if (transactionMessage.hasFileDelete()) { // file delete -------------
					final var id = transactionMessage.getFileDelete().getFileID().getFileNum();
					final Entity entity = entities.get(id);
					if (entity != null) {
						// TODO handle extra data from transactionMessage.getContractDeleteInstance();
						entity.setConsensusTimestamp(consensusTimestampNanosLong);
						entity.getFields().add("deleted",false);
						records.add(entity.asAvro());
					} else {
						System.err.println("Tried to update file 0.0."+id+" but had not been created");
					}
				}
				// TODO handle other transactions for create/update/delete of topics, tokens, etc
			} catch (InvalidProtocolBufferException e) {
				throw new RuntimeException(e);
			}
		}
		return records;
	}
	
	@NotNull
	private Entity accountToEntity(final TransactionRecord transactionRecord, final TransactionBody transactionMessage,
			final long consensusTimestampNanosLong) {
		final CryptoCreateTransactionBody createAccount = transactionMessage.getCryptoCreateAccount();
		final JsonObjectBuilder extraFields = Json.createObjectBuilder()
				.add("realm", transactionRecord.getReceipt().getAccountID().getRealmNum())
				.add("shard", transactionRecord.getReceipt().getAccountID().getShardNum());
		// TODO, where from? extraFields.add("submit_key","null")
		if (createAccount.hasProxyAccountID()) extraFields.add("proxy_account_id",createAccount.getProxyAccountID().toString());
		// TODO, where from? extraFields.add("created_timestamp","null")
		// TODO, where from? extraFields.add("auto_renew_account_id","null")
		extraFields.add("decline_reward",createAccount.getDeclineReward());
		extraFields.add("deleted",false);
		// TODO, where from? extraFields.add("ethereum_nonce","null")
		// TODO, where from? extraFields.add("expiry_timestamp","null")
		// TODO, where from? extraFields.add("max_automatic_token_associations","null")
		extraFields.add("max_automatic_token_associations",createAccount.getMaxAutomaticTokenAssociations());
		extraFields.add("memo",createAccount.getMemo());
		extraFields.add("receiver_sig_required",createAccount.getReceiverSigRequired());
		if (createAccount.hasStakedAccountId()) extraFields.add("staked_account_id",accountIdToString(createAccount.getStakedAccountId()));
		if (createAccount.hasStakedNodeId()) extraFields.add("staked_node_id",createAccount.getStakedNodeId());
		// TODO, where from? extraFields.add("stake_period_start","null")
		return new Entity(
				consensusTimestampNanosLong,
				transactionRecord.getReceipt().getAccountID().getAccountNum(),
				null, // TODO evm address?
				null, // TODO alias?
				Entity.Type.account,
				keyToJson(createAccount.getKey()),
				extraFields
		);
	}
	
	private void updateAccount(final TransactionBody transactionMessage, final Entity entity) {
		final JsonObjectBuilder extraFields = entity.getFields();
		final CryptoUpdateTransactionBody updateAccount = transactionMessage.getCryptoUpdateAccount();
		// TODO, where from? extraFields.add("submit_key","null")
		if (updateAccount.hasProxyAccountID()) {
			extraFields.add("proxy_account_id",Json.createValue(updateAccount.getProxyAccountID().toString()));
		}
		// TODO, where from? extraFields.add("created_timestamp","null")
		// TODO, where from? extraFields.add("auto_renew_account_id","null")
		if (updateAccount.hasDeclineReward()) {
			extraFields.add("decline_reward", updateAccount.getDeclineReward().getValue() ? JsonValue.TRUE : JsonValue.FALSE);
		}
		extraFields.add("deleted",JsonValue.FALSE);
		// TODO, where from? extraFields.add("ethereum_nonce","null")
		// TODO, where from? extraFields.add("expiry_timestamp","null")
		// TODO, where from? extraFields.add("max_automatic_token_associations","null")
		if (updateAccount.hasMaxAutomaticTokenAssociations()) {
			extraFields.add("max_automatic_token_associations",Json.createValue(updateAccount.getMaxAutomaticTokenAssociations().getValue()));
		}
		if (updateAccount.hasMemo()) {
			extraFields.add("memo",Json.createValue(updateAccount.getMemo().getValue()));
		}
		if (updateAccount.hasReceiverSigRequired()) {
			extraFields.add("receiver_sig_required", updateAccount.getReceiverSigRequired() ? JsonValue.TRUE : JsonValue.FALSE);
		}
		if (updateAccount.hasStakedAccountId()) {
			extraFields.add("staked_account_id",Json.createValue(accountIdToString(updateAccount.getStakedAccountId())));
		}
		if (updateAccount.hasStakedNodeId()) {
			extraFields.add("staked_node_id",Json.createValue(updateAccount.getStakedNodeId()));
		}
		// TODO, where from? extraFields.add("stake_period_start","null")
		if (updateAccount.hasKey()) {
			entity.setPublicKey(keyToJson(updateAccount.getKey()));
		}
	}

	@NotNull
	private Entity tokenToEntity(final TransactionRecord transactionRecord, final TransactionBody transactionMessage,
			final long consensusTimestampNanosLong) {
		final TokenCreateTransactionBody tokenCreation = transactionMessage.getTokenCreation();
		final JsonObjectBuilder extraFields = Json.createObjectBuilder()
						.add("realm", transactionRecord.getReceipt().getTokenID().getRealmNum())
						.add("shard", transactionRecord.getReceipt().getTokenID().getShardNum());
		extraFields.add("name",tokenCreation.getName());
		extraFields.add("symbol",tokenCreation.getSymbol());
		extraFields.add("decimals",tokenCreation.getDecimals());
		extraFields.add("initialSupply",tokenCreation.getInitialSupply());
		if (tokenCreation.hasTreasury()) extraFields.add("treasury",accountIdToString(tokenCreation.getTreasury()));
		if (tokenCreation.hasAdminKey()) extraFields.add("adminKey", keyToJson(tokenCreation.getAdminKey()));
		if (tokenCreation.hasKycKey()) extraFields.add("kycKey", keyToJson(tokenCreation.getKycKey()));
		if (tokenCreation.hasFreezeKey()) extraFields.add("freezeKey", keyToJson(tokenCreation.getFreezeKey()));
		if (tokenCreation.hasWipeKey()) extraFields.add("wipeKey", keyToJson(tokenCreation.getWipeKey()));
		if (tokenCreation.hasSupplyKey()) extraFields.add("supplyKey", keyToJson(tokenCreation.getSupplyKey()));
		if (tokenCreation.hasPauseKey()) extraFields.add("pauseKey", keyToJson(tokenCreation.getPauseKey()));
		extraFields.add("freezeDefault",tokenCreation.getFreezeDefault());
		if (tokenCreation.hasExpiry()) extraFields.add("expiry",timeStampToString(tokenCreation.getExpiry()));
		if (tokenCreation.hasAutoRenewAccount()) extraFields.add("autoRenewAccount",accountIdToString(tokenCreation.getAutoRenewAccount()));
		if (tokenCreation.hasAutoRenewPeriod()) extraFields.add("autoRenewPeriod",tokenCreation.getAutoRenewPeriod().getSeconds());
		extraFields.add("memo",tokenCreation.getMemo());
		extraFields.add("tokenType",tokenCreation.getTokenType().toString());
		extraFields.add("supplyType",tokenCreation.getSupplyType().toString());
		extraFields.add("maxSupply",tokenCreation.getMaxSupply());
		if (tokenCreation.hasFeeScheduleKey()) extraFields.add("feeScheduleKey", keyToJson(tokenCreation.getFeeScheduleKey()));
		if (tokenCreation.hasFeeScheduleKey()) extraFields.add("feeScheduleKey", keyToJson(tokenCreation.getFeeScheduleKey()));
		return new Entity(
				consensusTimestampNanosLong,
				transactionRecord.getReceipt().getTokenID().getTokenNum(),
				null, // TODO evm address?
				null, // TODO alias?
				Entity.Type.token,
				keyToJson(tokenCreation.getAdminKey()),
				extraFields
		);
	}

	private void updateToken(final TransactionBody transactionMessage,final Entity entity) {
		final JsonObjectBuilder extraFields = entity.getFields();
		final TokenUpdateTransactionBody tokenUpdate = transactionMessage.getTokenUpdate();
		extraFields.add("name",Json.createValue(tokenUpdate.getName()));
		extraFields.add("symbol",Json.createValue(tokenUpdate.getSymbol()));
		if (tokenUpdate.hasTreasury()) extraFields.add("treasury",Json.createValue(accountIdToString(tokenUpdate.getTreasury())));
		if (tokenUpdate.hasAdminKey()) {
			extraFields.add("adminKey", keyToJson(tokenUpdate.getAdminKey()));
			entity.setPublicKey(keyToJson(tokenUpdate.getAdminKey()));
		}
		if (tokenUpdate.hasKycKey()) extraFields.add("kycKey", keyToJson(tokenUpdate.getKycKey()));
		if (tokenUpdate.hasFreezeKey()) extraFields.add("freezeKey", keyToJson(tokenUpdate.getFreezeKey()));
		if (tokenUpdate.hasWipeKey()) extraFields.add("wipeKey", keyToJson(tokenUpdate.getWipeKey()));
		if (tokenUpdate.hasSupplyKey()) extraFields.add("supplyKey", keyToJson(tokenUpdate.getSupplyKey()));
		if (tokenUpdate.hasPauseKey()) extraFields.add("pauseKey", keyToJson(tokenUpdate.getPauseKey()));
		if (tokenUpdate.hasExpiry()) extraFields.add("expiry",Json.createValue(timeStampToString(tokenUpdate.getExpiry())));
		if (tokenUpdate.hasAutoRenewAccount()) extraFields.add("autoRenewAccount",Json.createValue(accountIdToString(tokenUpdate.getAutoRenewAccount())));
		if (tokenUpdate.hasAutoRenewPeriod()) extraFields.add("autoRenewPeriod",Json.createValue(tokenUpdate.getAutoRenewPeriod().getSeconds()));
		if (tokenUpdate.hasMemo()) extraFields.add("memo",Json.createValue(tokenUpdate.getMemo().toString()));
		if (tokenUpdate.hasFeeScheduleKey()) extraFields.add("feeScheduleKey", keyToJson(tokenUpdate.getFeeScheduleKey()));
		if (tokenUpdate.hasFeeScheduleKey()) extraFields.add("feeScheduleKey", keyToJson(tokenUpdate.getFeeScheduleKey()));
	}
	@NotNull
	private Entity topicToEntity(final TransactionRecord transactionRecord, final TransactionBody transactionMessage,
			final long consensusTimestampNanosLong) {
		final ConsensusCreateTopicTransactionBody topicCreation = transactionMessage.getConsensusCreateTopic();
		final JsonObjectBuilder extraFields = Json.createObjectBuilder()
						.add("realm", transactionRecord.getReceipt().getTopicID().getRealmNum())
						.add("shard", transactionRecord.getReceipt().getTopicID().getShardNum());
		extraFields.add("memo",topicCreation.getMemo());
		if (topicCreation.hasAdminKey()) extraFields.add("adminKey", keyToJson(topicCreation.getAdminKey()));
		if (topicCreation.hasSubmitKey()) extraFields.add("submitKey", keyToJson(topicCreation.getSubmitKey()));
		if (topicCreation.hasAutoRenewAccount()) extraFields.add("autoRenewAccount",accountIdToString(topicCreation.getAutoRenewAccount()));
		if (topicCreation.hasAutoRenewPeriod()) extraFields.add("autoRenewPeriod",topicCreation.getAutoRenewPeriod().getSeconds());
		return new Entity(
				consensusTimestampNanosLong,
				transactionRecord.getReceipt().getTokenID().getTokenNum(),
				null, // TODO evm address?
				null, // TODO alias?
				Entity.Type.topic,
				keyToJson(topicCreation.getAdminKey()),
				extraFields
		);
	}
	private void updateTopic(final TransactionBody transactionMessage,final Entity entity) {
		final JsonObjectBuilder extraFields = entity.getFields();
		final ConsensusUpdateTopicTransactionBody topicUpdate = transactionMessage.getConsensusUpdateTopic();
		if (topicUpdate.hasMemo()) extraFields.add("memo",Json.createValue(topicUpdate.getMemo().toString()));
		if (topicUpdate.hasAdminKey()) {
			extraFields.add("adminKey", keyToJson(topicUpdate.getAdminKey()));
			entity.setPublicKey(keyToJson(topicUpdate.getAdminKey()));
		}
		if (topicUpdate.hasSubmitKey()) extraFields.add("submitKey", keyToJson(topicUpdate.getSubmitKey()));
		if (topicUpdate.hasAutoRenewAccount()) extraFields.add("autoRenewAccount",Json.createValue(accountIdToString(topicUpdate.getAutoRenewAccount())));
		if (topicUpdate.hasAutoRenewPeriod()) extraFields.add("autoRenewPeriod",Json.createValue(topicUpdate.getAutoRenewPeriod().getSeconds()));
	}
	@NotNull
	private Entity contractToEntity(final TransactionRecord transactionRecord, final TransactionBody transactionMessage,
			final long consensusTimestampNanosLong) {
		final ContractCreateTransactionBody contractCreation = transactionMessage.getContractCreateInstance();
		final JsonObjectBuilder extraFields = Json.createObjectBuilder()
						.add("realm", transactionRecord.getReceipt().getContractID().getRealmNum())
						.add("shard", transactionRecord.getReceipt().getContractID().getShardNum());
		if (contractCreation.hasFileID()) {
			extraFields.add("fileId", contractCreation.getFileID().getShardNum()+"."+contractCreation.getFileID().getRealmNum()+"."+contractCreation.getFileID().getFileNum());
		}
		if (contractCreation.hasInitcode()) {
			extraFields.add("initCode",contractCreation.getInitcode().toString());
		}
		if (contractCreation.hasAdminKey()) extraFields.add("adminKey", keyToJson(contractCreation.getAdminKey()));
		extraFields.add("gas",contractCreation.getGas());
		extraFields.add("initialBalance",contractCreation.getInitialBalance());
		if (contractCreation.hasAutoRenewPeriod()) {
			extraFields.add("proxyAccountID",accountIdToString(contractCreation.getProxyAccountID()));
		}
		if (contractCreation.hasAutoRenewPeriod()) extraFields.add("autoRenewPeriod",contractCreation.getAutoRenewPeriod().getSeconds());
		extraFields.add("constructorParameters",contractCreation.getConstructorParameters().toString());
		if (contractCreation.hasShardID()) {
			extraFields.add("shardID",contractCreation.getShardID().getShardNum());
		}
		if (contractCreation.hasRealmID()) {
			extraFields.add("realmID",contractCreation.getRealmID().getShardNum()+"."+contractCreation.getRealmID().getRealmNum());
		}
		if (contractCreation.hasNewRealmAdminKey()) extraFields.add("newRealmAdminKey", keyToJson(contractCreation.getNewRealmAdminKey()));
		extraFields.add("memo",contractCreation.getMemo());
		extraFields.add("maxAutomaticTokenAssociations",contractCreation.getMaxAutomaticTokenAssociations());
		if (contractCreation.hasAutoRenewAccountId()) {
			extraFields.add("autoRenewAccountId",accountIdToString(contractCreation.getAutoRenewAccountId()));
		}
		if (contractCreation.hasStakedAccountId()) {
			extraFields.add("stakedAccountId",accountIdToString(contractCreation.getStakedAccountId()));
		}
		if (contractCreation.hasStakedNodeId()) {
			extraFields.add("stakedNodeId",contractCreation.getStakedNodeId());
		}
		extraFields.add("declineReward",contractCreation.getDeclineReward());
		return new Entity(
				consensusTimestampNanosLong,
				transactionRecord.getReceipt().getContractID().getContractNum(),
				null, // TODO evm address?
				null, // TODO alias?
				Entity.Type.contract,
				keyToJson(contractCreation.getAdminKey()),
				extraFields
		);
	}
	private void updateContract(final TransactionBody transactionMessage,final Entity entity) {
		final JsonObjectBuilder extraFields = entity.getFields();
		final ContractUpdateTransactionBody contractUpdate = transactionMessage.getContractUpdateInstance();
		if (contractUpdate.hasAdminKey()) {
			extraFields.add("adminKey", keyToJson(contractUpdate.getAdminKey()));
			entity.setPublicKey(keyToJson(contractUpdate.getAdminKey()));
		}
		if (contractUpdate.hasAutoRenewPeriod()) {
			extraFields.add("proxyAccountID",Json.createValue(accountIdToString(contractUpdate.getProxyAccountID())));
		}
		if (contractUpdate.hasAutoRenewPeriod()) extraFields.add("autoRenewPeriod",Json.createValue(contractUpdate.getAutoRenewPeriod().getSeconds()));
		if (contractUpdate.hasFileID()) {
			extraFields.add("fileId", Json.createValue(contractUpdate.getFileID().getShardNum()+"."+contractUpdate.getFileID().getRealmNum()+"."+contractUpdate.getFileID().getFileNum()));
		}
		if (contractUpdate.hasMemo()) extraFields.add("memo",Json.createValue(contractUpdate.getMemo()));
		if (contractUpdate.hasMemoWrapper()) extraFields.add("memo",Json.createValue(contractUpdate.getMemoWrapper().toString()));
		if (contractUpdate.hasMaxAutomaticTokenAssociations()) {
			extraFields.add("maxAutomaticTokenAssociations",Json.createValue(contractUpdate.getMaxAutomaticTokenAssociations().getValue()));
		}
		if (contractUpdate.hasAutoRenewAccountId()) {
			extraFields.add("autoRenewAccountId",Json.createValue(accountIdToString(contractUpdate.getAutoRenewAccountId())));
		}
		if (contractUpdate.hasStakedAccountId()) {
			extraFields.add("stakedAccountId",Json.createValue(accountIdToString(contractUpdate.getStakedAccountId())));
		}
		if (contractUpdate.hasStakedNodeId()) {
			extraFields.add("stakedNodeId",Json.createValue(contractUpdate.getStakedNodeId()));
		}
		if (contractUpdate.hasDeclineReward()) extraFields.add("declineReward",contractUpdate.getDeclineReward().getValue() ? JsonValue.TRUE : JsonValue.FALSE);
	}

	@NotNull
	private Entity fileToEntity(final TransactionRecord transactionRecord, final TransactionBody transactionMessage,
			final long consensusTimestampNanosLong) {
		final FileCreateTransactionBody fileCreate = transactionMessage.getFileCreate();
		final JsonObjectBuilder extraFields = Json.createObjectBuilder()
						.add("realm", transactionRecord.getReceipt().getFileID().getRealmNum())
						.add("shard", transactionRecord.getReceipt().getFileID().getShardNum());
		if (fileCreate.hasExpirationTime()) {
			extraFields.add("expirationTime", timeStampToString(fileCreate.getExpirationTime()));
		}
		if (fileCreate.hasKeys()) extraFields.add("keys", keyToJson(fileCreate.getKeys()));
		extraFields.add("contents",fileCreate.getContents().toString());
		if (fileCreate.hasShardID()) {
			extraFields.add("shardID",fileCreate.getShardID().getShardNum());
		}
		if (fileCreate.hasRealmID()) {
			extraFields.add("realmID",fileCreate.getRealmID().getShardNum()+"."+fileCreate.getRealmID().getRealmNum());
		}
		if (fileCreate.hasNewRealmAdminKey()) extraFields.add("newRealmAdminKey", keyToJson(fileCreate.getNewRealmAdminKey()));
		extraFields.add("memo",fileCreate.getMemo());
		return new Entity(
				consensusTimestampNanosLong,
				transactionRecord.getReceipt().getFileID().getFileNum(),
				null, // TODO evm address?
				null, // TODO alias?
				Entity.Type.file,
				fileCreate.hasKeys() ? keyToJson(fileCreate.getKeys()) : JsonValue.EMPTY_JSON_ARRAY,
				extraFields
		);
	}
	private void appendFile(final TransactionBody transactionMessage,final Entity entity) {
		final JsonObjectBuilder extraFields = entity.getFields();
		final FileAppendTransactionBody fileAppend = transactionMessage.getFileAppend();
		String oldContents = "";
		final var contents = extraFields.build().get("contents");
		if (contents != null && contents != JsonValue.NULL) {
			oldContents = contents.toString();
		}
		extraFields.add("contents",contents+fileAppend.getContents().toString());
	}
	private void updateFile(final TransactionBody transactionMessage,final Entity entity) {
		final JsonObjectBuilder extraFields = entity.getFields();
		final FileUpdateTransactionBody fileUpdate = transactionMessage.getFileUpdate();

		if (fileUpdate.hasExpirationTime()) {
			extraFields.add("expirationTime", Json.createValue(timeStampToString(fileUpdate.getExpirationTime())));
		}
		if (fileUpdate.hasKeys()) extraFields.add("keys", keyToJson(fileUpdate.getKeys()));
		if (fileUpdate.hasMemo()) extraFields.add("memo",Json.createValue(fileUpdate.getMemo().toString()));
	}

	public static JsonStructure keyToJson(Key key) {
		if (key.hasECDSA384()) {
			return Json.createObjectBuilder()
					.add("type","ECDSA384")
					.add("key",key.getECDSA384().toString())
					.build();
		} else if (key.hasECDSASecp256K1()) {
			return Json.createObjectBuilder()
					.add("type","ECDSASecp256K1")
					.add("key",key.getECDSASecp256K1().toString())
					.build();
		} else if (key.hasEd25519()) {
			return Json.createObjectBuilder()
					.add("type","Ed25519")
					.add("key",key.getEd25519().toString())
					.build();
		} else if (key.hasRSA3072()) {
			return Json.createObjectBuilder()
					.add("type","RSA3072")
					.add("key",key.getRSA3072().toString())
					.build();
		} else if (key.hasContractID()) {
			return Json.createObjectBuilder()
					.add("type","contractID")
					.add("key",key.getContractID().getShardNum()+"."+key.getContractID().getRealmNum()+"."+key.getContractID().getContractNum())
					.build();
		} else if (key.hasDelegatableContractId()) {
			return Json.createObjectBuilder()
					.add("type","delegatableContractId")
					.add("key",key.getDelegatableContractId().getShardNum()+"."+key.getDelegatableContractId().getRealmNum()+"."+key.getDelegatableContractId().getContractNum())
					.build();
		} else if (key.hasKeyList()) {
			return keyToJson(key.getKeyList());
		} else {
			// TODO handle unknown key
			return Json.createObjectBuilder()
					.build();
		}
	}
	public static JsonStructure keyToJson(KeyList keyList) {
		final JsonArrayBuilder array = Json.createArrayBuilder();
		for(Key subKey: keyList.getKeysList()) {
			array.add(keyToJson(subKey));
		}
		return array.build();
	}

	public static String accountIdToString(AccountID accountID) {
		if (accountID.hasAlias()) {
			return accountID.getShardNum()+"."+accountID.getRealmNum()+"."+accountID.getAlias();
		} else {
			return accountID.getShardNum()+"."+accountID.getRealmNum()+"."+accountID.getAccountNum();
		}
	}

	public static String timeStampToString(Timestamp timestamp) {
		return timestamp.getSeconds()+"."+timestamp.getNanos();
	}
}
