package com.nomistech.kafka_play.tutorial1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            // Create producer record
            String topic = "my-topic-002";
            String key   = "id_" + i;
            String value = "Hello world " + i;
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key );
            // Observe that the same key goes to the same partition on every
            // run.

            // Send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        // The record was successfully sent.
                        logger.info("Received new metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            })
            // The `get` forces things to be synchronous and makes it easier
            // for us to see what is happening.
            // Don't do this in real code -- it will make things slow.
            .get();
        }
        producer.flush();
        producer.close();
    }
}
