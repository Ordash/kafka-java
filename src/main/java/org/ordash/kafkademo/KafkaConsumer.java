package org.ordash.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class.getSimpleName());

	public static void main(String[] args) {

		log.info("I am a Kafka Consumer");

		String groupId = "my-java-app";
		String topic = "demo_java";

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

		// Consumer configs
		props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

		// set consumer group
		props.setProperty("group.id", groupId);

		// set offset read from - "none" -> if you don't have any consumer group it will fail, "earliest" means will read from the beginning, "latest" means will read from the end
		props.setProperty("auto.offset.reset", "earliest");

		// create consumer
		org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

		consumer.subscribe(Arrays.asList(topic));

		while (true) {
			log.info("polling");

			ConsumerRecords<String, String> records =
					consumer.poll(Duration.ofMillis(1000));

			for (ConsumerRecord<String, String> record : records) {
				log.info("Key: {}, Value: {}", record.key(), record.value());
				log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
			}

		}






	}

}
