package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // Creating producer properties
        Properties  properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Creating producer
        KafkaProducer<String, String> kafkaProducers = new KafkaProducer<String, String>(properties);

        // Creating a producer record
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "This is a Java Producer Test");


        // Send Data - Happens Asynchronously
        kafkaProducers.send(record);

        // flush and send data
        kafkaProducers.flush();
    }
}
