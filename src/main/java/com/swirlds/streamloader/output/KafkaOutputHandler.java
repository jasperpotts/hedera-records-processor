package com.swirlds.streamloader.output;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.json.JsonObject;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaOutputHandler implements OutputHandler {
	private final static String TRANSACTIONS_TOPIC = "transaction_record_new";
	private final static String RECORDS_TOPIC = "record_file_new";
	private final Producer<String, String> producer;

	private final ConcurrentLinkedDeque<Future<RecordMetadata>> futures = new ConcurrentLinkedDeque<>();

	public KafkaOutputHandler(String serverIP) {
		Properties props = new Properties();
		props.put("bootstrap.servers", serverIP+":9092");
		props.put("linger.ms", 1000);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);

		final Thread resultsHandlingThread = new Thread(() -> {
			while(true) {
				Future<RecordMetadata> future = futures.pollFirst();
				if (future != null) {
					try {
						future.get();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
				} else {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				}
			}
		}, "Kafka-Results-Handler");
		resultsHandlingThread.start();
	}

	@Override
	public void outputTransaction(final JsonObject transactionJson) {
		futures.add(producer.send(new ProducerRecord<>(TRANSACTIONS_TOPIC,transactionJson.toString())));
	}


	@Override
	public void outputRecordFile(final JsonObject recordFileJson) {
		futures.add(producer.send(new ProducerRecord<>(RECORDS_TOPIC,recordFileJson.toString())));
	}

	@Override
	public void close() {
		System.out.println("KafkaOutputHandler.close =============");
//		producer.close();
	}
}
