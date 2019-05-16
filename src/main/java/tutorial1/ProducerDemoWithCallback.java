package tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;


public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        // Creating a Logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";

        // Creating producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Creating producer
        KafkaProducer<String, String> kafkaProducers = new KafkaProducer<String, String>(properties);

        // Creating a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "This is a Java Producer Test");


        // Send Data - Happens Asynchronously
        kafkaProducers.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // Executes every time a message is sent successfully or an exception is thrown
                if(e == null) {
                    logger.info("Received new metadata \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                }
                else {
                    logger.error("Errors", e);
                }
            }
        });

        // flush and send data
        kafkaProducers.flush();
    }
}
