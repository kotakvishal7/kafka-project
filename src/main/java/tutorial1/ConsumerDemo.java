package tutorial1;

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

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        // Create Properties
        Properties properties = new Properties();

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-first-group";
        String topic = "first_topic";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to our topics
        kafkaConsumer.subscribe(Arrays.asList(topic));

        // poll for data
        while(true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records) {
                logger.info("Key " + record.key() + " Value " + record.value());
                logger.info("Partition " + record.partition() + ", Offset " + record.offset());
            }
        }

    }
}
