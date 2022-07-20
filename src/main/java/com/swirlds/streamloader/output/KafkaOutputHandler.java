package com.swirlds.streamloader.output;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.json.JsonObject;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaOutputHandler implements OutputHandler {
	private final static String TRANSACTIONS_TOPIC = "transaction_record_new";
	private final static String RECORDS_TOPIC = "record_file_new";
	private final Producer<String, String> producer;

	private final ArrayBlockingQueue<Future<RecordMetadata>> futures = new ArrayBlockingQueue<>(10_000);
	private final AtomicBoolean finished = new AtomicBoolean(false);

	public KafkaOutputHandler(String serverIP) {
		Properties props = new Properties();
		props.put("bootstrap.servers", serverIP+":9092");
		props.put("linger.ms", 1000);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);

		final Thread resultsHandlingThread = new Thread(() -> {
			while(!finished.get()) {
				try {
					Future<RecordMetadata> future = futures.take();
					future.get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}, "Kafka-Results-Handler");
		resultsHandlingThread.start();

		// give producer a couple seconds to connect
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
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
		finished.set(true);
		producer.close();
	}
}
