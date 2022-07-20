package com.swirlds.streamloader.output;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.json.JsonObject;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaBatchedOutputHandler implements OutputHandler {
	private final static int BATCH_SIZE = 800_000;
	private final static String TRANSACTIONS_TOPIC = "transaction_record_new";
	private final static String RECORDS_TOPIC = "record_file_new";
	private final static String BALANCES_TOPIC = "balance";
	private final Producer<String, String> producer;

	private final ArrayBlockingQueue<Future<RecordMetadata>> futures = new ArrayBlockingQueue<>(10_000);
	private final AtomicBoolean finished = new AtomicBoolean(false);

	private final AtomicLong transactionMessageCount = new AtomicLong(0);
	private final AtomicLong recordFileMessageCount = new AtomicLong(0);
	private final AtomicLong balanceMessageCount = new AtomicLong(0);

	private final StringBuilder transactionRows = new StringBuilder();
	private final StringBuilder recordFileRows = new StringBuilder();
	private final StringBuilder balanceRows = new StringBuilder();

	public KafkaBatchedOutputHandler(String serverIP) {
		Properties props = new Properties();
		props.put("bootstrap.servers", serverIP+":9092");
		props.put("linger.ms", 10000);
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
		batchSend(TRANSACTIONS_TOPIC,transactionJson,transactionRows, transactionMessageCount);
	}

	@Override
	public void outputRecordFile(final JsonObject recordFileJson) {
		batchSend(RECORDS_TOPIC,recordFileJson,recordFileRows, recordFileMessageCount);
	}

	@Override
	public void outputAccountBalance(final JsonObject balanceJson) {
		batchSend(BALANCES_TOPIC,balanceJson,balanceRows, balanceMessageCount);
	}

	@Override
	public void close() {
		System.out.println("KafkaOutputHandler.close =============");
		finished.set(true);
		producer.close();
	}

	private void batchSend(String topic, JsonObject newJsonRow, StringBuilder stringBuilder, AtomicLong count) {
		if (stringBuilder.length() >= BATCH_SIZE) {
			try {
				futures.put(producer.send(new ProducerRecord<>(topic,Long.toString(count.incrementAndGet()),stringBuilder.toString())));
				stringBuilder.setLength(0);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
		stringBuilder.append(newJsonRow.toString());
		stringBuilder.append('\n');
	}
}
