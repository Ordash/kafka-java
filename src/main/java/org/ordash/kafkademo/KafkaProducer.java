package org.ordash.kafkademo;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducer {

	private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class.getSimpleName());

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

		// Create producer
		org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);

        // Producer record
		ProducerRecord<String, String> record = new ProducerRecord<>("demo_java", "Please only one");

        // Send record
        producer.send(record);

        // Close producer
        producer.close();

	}

}
