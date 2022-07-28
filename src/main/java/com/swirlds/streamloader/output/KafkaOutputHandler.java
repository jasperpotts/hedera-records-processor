package com.swirlds.streamloader.output;

import com.swirlds.streamloader.data.JsonRow;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.Properties;

public class KafkaOutputHandler implements OutputHandler<JsonRow> {
	private final Producer<String, String> producer;

	public KafkaOutputHandler(String serverIP) {
		Properties props = new Properties();
		props.put("bootstrap.servers", serverIP+":9092");
		props.put("acks", "0"); // for now, we do not need guaranteed deliveries
		props.put("linger.ms", 10000);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);

		// give producer a couple seconds to connect
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void accept(final List<JsonRow> jsonRows) {
		for(var row: jsonRows) {
			producer.send(new ProducerRecord<>(row.topicTableName(), row.json()), (record, error) -> {
				if (error != null) error.printStackTrace();
			});
		}
	}

	@Override
	public void close() {
		producer.close();
	}
}
