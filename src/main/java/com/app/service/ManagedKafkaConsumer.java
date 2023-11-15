package com.app.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.util.Collections;
import java.util.Properties;


@Service
public class ManagedKafkaConsumer {

    private KafkaConsumer<String, String> consumer;
    private final String topicName;
    private volatile boolean running = true;

    public ManagedKafkaConsumer(@Value("${kafka.topic}") String topicName) {
        this.topicName = topicName;
    }

    @PostConstruct
    public void start() {
        Properties props = new Properties();

        // Set the Kafka bootstrap servers
        props.put("bootstrap.servers", "localhost:9092");

        // Set the consumer group id
        props.put("group.id", "movie-consumer-group");

        // Specify the key and value deserializers
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(topicName));
        Thread consumerThread = new Thread(this::runConsumer);
        consumerThread.start();
    }

    private void runConsumer() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed record with Key: %s, Value: %s, Topic: %s, Partition: %d, Offset: %d%n",
                            record.key(), record.value(), record.topic(), record.partition(), record.offset());
                }
            }
            }
        finally {
            consumer.close();
        }
    }

    @PreDestroy
    public void shutdown() {
        running = false;
    }
}

