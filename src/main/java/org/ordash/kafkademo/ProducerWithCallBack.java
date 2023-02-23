package org.ordash.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallBack {

	private static final Logger log = LoggerFactory.getLogger(ProducerWithCallBack.class.getSimpleName());

	public static void main(String[] args) {

		// Create producer props
		Properties props = new Properties();

		// Localhost
		//props.setProperty("bootstrap.servers", "127.0.0.1:9092");


		// Conduktor playground
		props.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
		props.setProperty("security.protocol", "SASL_SSL");
		props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
				"username=\"68dk5Vysn08R92mEmPqVO5\" password=\"" +
				"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGlj" +
				"YXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2OGRrNVZ5c24wOFI5Mm" +
				"1FbVBxVk81Iiwib3JnYW5pemF0aW9uSWQiOjcwNTY3LCJ1c2VySWQiOjgxNjY5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJhYmY4ZGV" +
				"kNy02ODUyLTQyMGMtOTg4ZS1iOWM5ZThkYjlkMjgifX0.n3tgzpIiiLDRzbZlhzx1_k3rVX6dg-8FnkWcL6u6in8\";");
		props.setProperty("sasl.mechanism", "PLAIN");


		// Producer props
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", StringSerializer.class.getName());

		// Set batch size
		props.setProperty("batch.size", "400");

		// Create producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);


		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 30; j++) {
				// Producer record
				ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "Hello OK World!" + j);

				// Send record
				producer.send(record, (recordMetadata, e) -> {
					// executes every time a record is successfully sent or an exception is thrown
					if (e != null) {
						log.error("Failed to send record", e);
					} else {
						log.info("""
						Successfully sent record:
						Topic: {},
						Partition: {},
						Offset: {},
						Timestamp: {},
						""", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
					}
				});
			}

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		// Close producer
        producer.close();
	}

}
