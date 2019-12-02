package com.github.OffS3c.kafka.java.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class.getName());

        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        String topic = "first_topic";

        for (int i = 0; i < 10; i++) {
            String key = "id_" + Integer.toString(i);
            String value = "sending message " + Integer.toString(i);

            // create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            // send data
            // producer.send(record);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // record sent or exception thrown
                    if (e == null) {
                        // success
                        logger.info(
                                "Received new metadata: \n" +
                                        "Topic: " + recordMetadata.topic() + "\n" +
                                        "Partition: " + recordMetadata.partition() + "\n" +
                                        "Offset: " + recordMetadata.offset() + "\n" +
                                        "TimeStamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        // failed
                    }
                }
            });
        }

        // flush and close
        producer.flush();
        producer.close();

    }
}
