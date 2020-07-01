package services;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Twitter_data_producer {
    String topic;
    Properties properties;
    KafkaProducer<String, String> producer;
    public Twitter_data_producer(String bootstrapServers, String topic){
        this.topic = topic;
        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void sendValue(String value){
        ProducerRecord<String,String> record = new ProducerRecord<>(topic, value);
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
