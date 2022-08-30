package com.swirlds.streamloader.util;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static com.swirlds.streamloader.util.GoogleStorageHelper.uploadFile;

public class ShiftAvroConsensusTimes {
	// copied from ./src/main/java/com/swirlds/streamloader/processing/TransactionProcessingBlock.java; check
	// there for latest version!
	private static final Schema TRANSACTION_AVRO_SCHEMA = new Schema.Parser().parse("""
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "transaction",
			 "fields": [
			     {"name": "consensus_timestamp", "type": "long"},
			     {"name": "entityId", "type": "long"},
			     {"name": "type", "type": "string"},
			     {"name": "index", "type": "long"},
			     {"name": "transaction_id", "type": "string"},
			     {"name": "result", "type": "string"},
			     {"name": "fields", "type": "string"},
			     {"name": "transfers_tokens", "type": "string"},
			     {"name": "transfers_hbar", "type": "string"},
			     {"name": "transfers_nft", "type": "string"},
			     {"name": "contract_logs", "type": "string", "default" : ""},
			     {"name": "contract_results", "type": "string", "default" : ""},
			     {"name": "contract_state_change", "type": "string"},
			     {"name": "nonce", "type": "int"},
			     {"name": "scheduled", "type": "boolean"},
			     {"name": "assessed_custom_fees", "type": "string"},
			     {"name": "ids", "type": {"type" : "array", "items" : "long"}},
			     {"name": "credited_ids", "type": {"type" : "array", "items" : "long"}},
			     {"name": "debited_ids", "type": {"type" : "array", "items" : "long"}}
			 ]
			}""");

	private static final boolean uploadAndRemoveGeneratedFiles = true;

	public static void main(String[] args) {
		Schema schema = TRANSACTION_AVRO_SCHEMA;
		List<String> jvmArgs = Arrays.asList(args);
		if (jvmArgs.size() != 3) {
			System.out.println("Usage: ShiftAvroConsensusTimes <inputFilename> <outputFilename> <# of 3-year offsets>");
			System.exit(1);
		}
		final String inputFilename = jvmArgs.get(0);
		final String outputFilename = jvmArgs.get(1);
		long offsetInNanos = 0L;
		try {
			int years = 3 * Integer.parseInt(jvmArgs.get(2));
			if (years < -15) {
				System.out.print("Warning: it is possible that your timestamps will end up pre-epoch (negative longs)");
				System.out.println(".\nThis is not necessarily an error - just be cautious.");
			}
			offsetInNanos = years * 365 * 24 * 3600 * 1000000000L;
			System.out.println("offset: " + offsetInNanos);
		} catch (NumberFormatException ex) {
			ex.printStackTrace();
			System.exit(2);
		}

		File inputFile = new File(inputFilename);
		File outputFile = new File(outputFilename);
		if (!inputFile.exists()) {
			System.out.println("Input file '" + inputFilename + "' does not exist!");
			System.exit(3);
		}
		if (outputFile.exists()) {
			System.out.println("Output file '" + outputFilename + "' already exists!");
			System.exit(4);
		}

		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
		try {
			dataFileWriter.create(schema, outputFile);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		GenericRecord record = null;
		GenericRecord outputRecord;
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
		long recordNumber = 0;
		long minimumInputTimestamp = Long.MAX_VALUE;
		long maximumInputTimestamp = Long.MIN_VALUE;
		long minimumOutputTimestamp = Long.MAX_VALUE;
		long maximumOutputTimestamp = Long.MIN_VALUE;
		try {
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(inputFile, datumReader);

			while (dataFileReader.hasNext()) {
				record = dataFileReader.next(record);
				outputRecord = new GenericData.Record(schema);
				Object consensusTimestampAsObject = record.get("consensus_timestamp");
				Long consensusTimestamp = 0L;
				try {
					if (consensusTimestampAsObject instanceof Long) {
							consensusTimestamp = ((Long) consensusTimestampAsObject).longValue();
					} else if (consensusTimestampAsObject instanceof Integer) {
							consensusTimestamp = ((Integer) consensusTimestampAsObject).longValue();
					} else if (consensusTimestampAsObject instanceof String) {
							consensusTimestamp = Long.valueOf((String) consensusTimestampAsObject);
					} else {
							System.out.println("Found invalid consensus_timestamp " + consensusTimestampAsObject +
							" in record #" + recordNumber + " of " + inputFilename);
					}
				} catch (Exception e) {
						System.out.println("Found invalid consensus_timestamp " + consensusTimestampAsObject +
								" in record #" + recordNumber + " of " + inputFilename);
				}
				minimumInputTimestamp = Math.min(minimumInputTimestamp, consensusTimestamp);
				maximumInputTimestamp = Math.max(minimumInputTimestamp, consensusTimestamp);
				consensusTimestamp += offsetInNanos;
				minimumOutputTimestamp = Math.min(minimumOutputTimestamp, consensusTimestamp);
				maximumOutputTimestamp = Math.max(minimumOutputTimestamp, consensusTimestamp);
				outputRecord.put("consensus_timestamp", consensusTimestamp);
				outputRecord.put("entityId", record.get("entityId"));
				outputRecord.put("type", record.get("type"));
				outputRecord.put("index", record.get("index"));
				outputRecord.put("transaction_id", record.get("transaction_id"));
				outputRecord.put("result", record.get("result"));
				outputRecord.put("fields", record.get("fields"));
				outputRecord.put("transfers_tokens", record.get("transfers_tokens"));
				outputRecord.put("transfers_hbar", record.get("transfers_hbar"));
				outputRecord.put("transfers_nft", record.get("transfers_nft"));
				outputRecord.put("contract_logs", record.get("contract_logs"));
				outputRecord.put("contract_results", record.get("contract_results"));
				outputRecord.put("contract_state_change", record.get("contract_state_change"));
				outputRecord.put("nonce", record.get("nonce"));
				outputRecord.put("scheduled", record.get("scheduled"));
				outputRecord.put("assessed_custom_fees", record.get("assessed_custom_fees"));
				outputRecord.put("ids", record.get("ids"));
				outputRecord.put("credited_ids", record.get("credited_ids"));
				outputRecord.put("debited_ids", record.get("debited_ids"));
				recordNumber++;
				dataFileWriter.append(outputRecord);
			}
			dataFileWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		System.out.println("A total of " + recordNumber + " records were copied/modified.");
		System.out.println("Input consensus timestamps ranged from " + minimumInputTimestamp + " to " +
				maximumInputTimestamp);
		System.out.println("Output consensus timestamps ranged from " + minimumOutputTimestamp + " to " +
				maximumOutputTimestamp);
		if (uploadAndRemoveGeneratedFiles) {
			try {
				uploadFile("pinot-ingestion", Paths.get(outputFilename), schema.getName() + "/" +
						outputFilename.substring(outputFilename.lastIndexOf("/") + 1));
				if (outputFile.delete()) {
					System.out.println("File " + outputFilename + " successfully uploaded and deleted locally.");
				} else {
					System.out.println("*** File " + outputFilename + " not successfully uploaded / deleted.");
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
