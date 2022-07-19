package com.swirlds.streamloader.output;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.json.JsonObject;
import java.util.Properties;

public class KafkaOutputHandler implements OutputHandler {
	private final static String TRANSACTIONS_TOPIC = "transaction_record_new";
	private final static String RECORDS_TOPIC = "record_file_new";
	private final Producer<String, String> producer;

	public KafkaOutputHandler(String serverIP) {
		Properties props = new Properties();
		props.put("bootstrap.servers", serverIP+":9092");
		props.put("linger.ms", 1000);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
	}

	@Override
	public void outputTransaction(final JsonObject transactionJson) {
		producer.send(new ProducerRecord<>(TRANSACTIONS_TOPIC,transactionJson.toString()));
	}


	@Override
	public void outputRecordFile(final JsonObject recordFileJson) {
		producer.send(new ProducerRecord<>(RECORDS_TOPIC,recordFileJson.toString()));
	}

	@Override
	public void close() {
		producer.close();
	}
}
