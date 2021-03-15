package kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreadNomis {

    final static String bootstrapServers = "127.0.0.1:9092";
    final static String groupId = "my-java-consumer-001-nomis";
    final static String topic = "demo-topic-001";

    public static void main(String[] args) {
        new ConsumerDemoWithThreadNomis().run();
    }

    static Properties properties = new Properties();

    private ConsumerDemoWithThreadNomis() {
        // Create consumer properties.
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // "earliest" is like "--from-beginning" in the CLI.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private void run() {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreadNomis.class.getName());
        logger.info("Creating the consumer thread");

        final CountDownLatch latch = new CountDownLatch(1);

        // Create the consumer runnable.
        ConsumerRunnable myConsumerRunnable = new ConsumerRunnable(latch);

        // Start the thread.
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Add a shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            myConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Exiting app");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application was interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;
        private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;
            // Create consumer.
            consumer = new KafkaConsumer<String, String>(properties);
            // Subscribe to topic(s).
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            // Poll for new data.
            try {
                while (true) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown(); // tells main code that we're done
            }

        }

        public void shutdown() {
            // This will cause the call to `poll` to throw a WakeUpException.
            consumer.wakeup();
        }
    }
}
