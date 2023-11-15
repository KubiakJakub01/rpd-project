package com.app.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaStockProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final String topicName = "movie_topic"; // Replace with your Kafka topic

    public void sendStockData(String symbol, String data) {
        kafkaTemplate.send(topicName, symbol, data);
    }

    // You can create additional methods for different types of data or topics
}

