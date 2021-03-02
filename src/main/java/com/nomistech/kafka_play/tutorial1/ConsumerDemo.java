package com.nomistech.kafka_play.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        final String bootstrapServers = "127.0.0.1:9092";
        final String groupId = "my-java-consumer-001";
        final String topic = "demo-topic-001";

        // Create consumer properties.
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // "earliest" is like "--from-beginning" in the CLI.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer.
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // Subscribe to topic(s).
        consumer.subscribe(Arrays.asList(topic));

        // Poll for new data.
        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
