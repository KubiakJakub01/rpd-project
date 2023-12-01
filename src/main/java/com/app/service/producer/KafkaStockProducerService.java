package com.app.service.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaStockProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendStockData(String topicName,String symbol, String data) {
        kafkaTemplate.send(topicName, symbol, data);
    }

    // You can create additional methods for different types of data or topics
}

