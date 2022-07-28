package com.swirlds.streamloader;


import com.swirlds.streamloader.util.Utils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroTest {
	private static final String BALANCES_AVRO_SCHEMA = """
			{"namespace": "com.swirlds",
			 "type": "record",
			 "name": "Balance",
			 "fields": [
			     {"name": "consensus_timestamp", "type": "long"},
			     {"name": "account_id", "type": "long"},
			     {"name": "token_id", "type": "long"},
			     {"name": "balance", "type": "long"}
			 ]
			}""";

	public static void main(String[] args) {
		Schema schema = new Schema.Parser().parse(BALANCES_AVRO_SCHEMA);

		File file = new File("build/EXAMPLE_BALANCES.avro");
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
		try {
			dataFileWriter.create(schema, file);
			long timeSeconds = System.currentTimeMillis() / 1000;
			for (int i = 0; i < 100; i++) {
				GenericRecord balance = new GenericData.Record(schema);
				balance.put("consensus_timestamp", Utils.getEpocNanosAsLong(timeSeconds,i));
				balance.put("account_id", (long)i);
				balance.put("token_id", 0L);
				balance.put("balance", 1_000_000L*i);
				dataFileWriter.append(balance);
			}
			dataFileWriter.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
