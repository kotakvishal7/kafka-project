package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class ProducerDemoKeys {

    public static void main(String[] args) {

        // Creating a Logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

        String bootstrapServers = "127.0.0.1:9092";

        // Creating producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Creating producer
        KafkaProducer<String, String> kafkaProducers = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello World" + Integer.toString(i);
            String key = "_id" + Integer.toString(i);

            // Creating a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);


            // Send Data - Happens Asynchronously
            kafkaProducers.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes every time a message is sent successfully or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Errors", e);
                    }
                }
            });
        }

        // flush and send data
        kafkaProducers.flush();
    }
}
