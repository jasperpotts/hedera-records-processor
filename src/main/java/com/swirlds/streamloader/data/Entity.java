package com.swirlds.streamloader.data;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import java.nio.ByteBuffer;
import java.util.Locale;

/**
Fields Column Format
{
  "realm": 0,
  "shard": 0,
  "submit_key ": "",
  "proxy_account_id ": "",
  "created_timestamp": "1970-01-01 00:00:00",
  "auto_renew_account_id": "",
  "auto_renew_period": 7776000,
  "decline_reward": false,
  "deleted": false,
  "ethereum_nonce": 0,
  "expiry_timestamp": null,
  "max_automatic_token_associations": 0,
  "memo": "",
  "receiver_sig_required": false,
  "staked_account_id": null,
  "staked_node_id": null,
  "stake_period_start": null,

  // Optional for Tokens
  "symbol":"",
  "tokenType":"Fungible",
  // Optional for Smart Contracts
  "file_id":0,
  "obtainer_id":0,
  "initcode": "0x0A722A700802126C0A221220791F524",
  "permanent_removal":"",
  // Optional for Scheduled Transactions
  "public_key_prefix":0,
  "signature":0,
  "type":"",
  "admin_key":""
}
 */
public final class Entity {
	public enum Type {
		account,token,contract,file,topic,scheduled
	}
	private static final Schema AVRO_SCHEMA = new Schema.Parser().parse("""
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "entity",
			 "fields": [
			     {"name": "consensus_timestamp", "type": "long"},
			     {"name": "entity_number", "type": "long"},
			     {"name": "evm_address", "type": "bytes", "default": ""},
			     {"name": "alias", "type": "bytes", "default": ""},
			     {"name": "type", "type": "string", "default": ""},
			     {"name": "public_key", "type": "string", "default": ""},
			     {"name": "fields", "type": "string", "default": ""}
			 ]
			}""");

	private long consensus_timestamp;
	private long entity_number;
	private byte[] evm_address;
	private byte[] alias;
	private Type type;
	private JsonStructure public_key;
	private final JsonObjectBuilder fields;

	public Entity() {
		this.consensus_timestamp = -1;
		this.entity_number = -1;
		this.evm_address = null;
		this.alias = null;
		this.type = null;
		this.public_key = JsonValue.EMPTY_JSON_OBJECT;
		this.fields = Json.createObjectBuilder();
	}

	public Entity(final long entity_number, final Type type) {
		this.consensus_timestamp = -1;
		this.entity_number = -1;
		this.evm_address = null;
		this.alias = null;
		this.type = type;
		this.public_key = JsonValue.EMPTY_JSON_OBJECT;
		this.fields = Json.createObjectBuilder()
				.add("realm", 0)
				.add("shard", 0);
	}

	public Entity(final long consensus_timestamp, final long entity_number, final byte[] evm_address,
			final byte[] alias,
			final Type type,
			final JsonStructure public_key, final JsonObjectBuilder fields) {
		this.consensus_timestamp = consensus_timestamp;
		this.entity_number = entity_number;
		this.evm_address = evm_address;
		this.alias = alias;
		this.type = type;
		this.public_key = public_key;
		this.fields = fields;
	}

	public long getConsensusTimestamp() {
		return consensus_timestamp;
	}

	public void setConsensusTimestamp(final long consensus_timestamp) {
		this.consensus_timestamp = consensus_timestamp;
	}

	public long getEntityNumber() {
		return entity_number;
	}

	public void setEntityNumber(final long entity_number) {
		this.entity_number = entity_number;
	}

	public byte[] getEvmAddress() {
		return evm_address;
	}

	public void setEvmAddress(final byte[] evm_address) {
		this.evm_address = evm_address;
	}

	public byte[] getAlias() {
		return alias;
	}

	public void setAlias(final byte[] alias) {
		this.alias = alias;
	}

	public Type getType() {
		return type;
	}

	public void setType(final Type type) {
		this.type = type;
	}

	public JsonStructure getPublicKey() {
		return public_key;
	}

	public void setPublicKey(final JsonStructure public_key) {
		this.public_key = public_key;
	}

	public JsonObjectBuilder getFields() {
		return fields;
	}

	public GenericRecord asAvro() {
		final var builder = new GenericRecordBuilder(AVRO_SCHEMA)
				.set("consensus_timestamp",consensus_timestamp)
				.set("entity_number",entity_number)
				.set("type",type.toString().toUpperCase(Locale.ROOT));
		if (alias != null) builder.set("alias", ByteBuffer.wrap(alias));
		if (evm_address != null) builder.set("evm_address",evm_address);
		if (public_key != null) builder.set("public_key",public_key.toString());
		if (fields != null) builder.set("fields",fields.build().toString());
		return builder.build();
	}
}
