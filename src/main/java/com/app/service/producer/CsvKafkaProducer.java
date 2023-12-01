package com.app.service.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;


@Service
public class CsvKafkaProducer {

    @Autowired
    private KafkaStockProducerService kafkaStockProducerService;

    @Value("${kafka.historical.topic}")
    private String topicName;

    public void sendCsvLineToKafka(String symbol, String line) {
        String message = convertRecordToJson(line);
        System.out.println("Producer written to csv msg: "+message);
        // Send data to Kafka
        kafkaStockProducerService.sendStockData(topicName, symbol, message);

    }

    private String convertRecordToJson(String line){
        String[] csvValues = line.split(",");
        Map<String, String> map = new HashMap<>();
        map.put("timestamp", csvValues[0]);
        map.put("open", csvValues[1]);
        map.put("high", csvValues[2]);
        map.put("low", csvValues[3]);
        map.put("close", csvValues[4]);
        map.put("volume", csvValues[5]);

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

}


