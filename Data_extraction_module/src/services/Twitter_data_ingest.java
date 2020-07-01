package services;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utility.Parser_json;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static utility.Post_request.postToURL;

public class Twitter_data_ingest {
    Properties properties;
    String groupId;
    String topic;
    public Twitter_data_ingest(String bootstrapServers, String groupId, String topic) {
        this.groupId = groupId;
        this.topic = topic;
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }


    public void retrieveDataFromProducer() throws IOException, ParseException {
        String url = "http://127.0.0.1:5000/api/insert-document";
        Logger logger = LoggerFactory.getLogger(Twitter_data_ingest.class.getName());
        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            String response = "";
            DefaultHttpClient httpClient = new DefaultHttpClient();

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("qwerty");
                if(record.value()!=null) {
                    logger.info("Key: " + record.key());
                    logger.info("Value: " + record.value());
                    logger.info("Partition: " + record.partition());
                    logger.info("offset: " + record.offset());

                    String message = record.value();
                    Parser_json parserJsonClass = new Parser_json();
                    System.out.println(message);
                    message = parserJsonClass.filter(message);
                    if (message.equals("-1")) {

                    } else {
                        response = postToURL(url, message, httpClient);
                        System.out.println(response);
                    }
                }
            }
            httpClient.getConnectionManager().shutdown();
        }

    }
}







