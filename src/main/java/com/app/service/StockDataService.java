package com.app.service;

import com.app.service.producer.KafkaStockProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@Service
public class StockDataService {

    @Value("${alphavantage.apikey}")
    private String topicName;

    @Autowired
    private AlphaVantageService alphaVantageService;

    @Autowired
    private KafkaStockProducerService kafkaStockProducerService;

    // default method without month
    public Map<String, Object> processAndSendStockData(String symbol) {
        // Fetch data
        Map<String, Object> stockData = alphaVantageService.getDailyTimeSeries(symbol);

        // Process data (optional, depending on your use case)
        String processedData = processStockData(stockData);

        // Send data to Kafka
        kafkaStockProducerService.sendStockData(topicName,symbol, processedData);

        // Return the fetched data
        return stockData;
    }

    // overloaded method for writing to kafka topic with month
    public Map<String, Object> processAndSendStockData(String symbol,String month) {
        // Fetch data
        Map<String, Object> stockData = alphaVantageService.getDailyTimeSeries(symbol,month);

        // Process data (optional, depending on your use case)
        String processedData = processStockData(stockData);

        // Send data to Kafka
        kafkaStockProducerService.sendStockData(topicName, symbol, processedData);

        // Return the fetched data
        return stockData;
    }

    private String processStockData(Map<String, Object> stockData) {
        // Implement your data processing logic here
        // For example, converting the Map to a JSON string
        return convertMapToJsonString(stockData);
    }

    private String convertMapToJsonString(Map<String, Object> map) {
        // Create an ObjectMapper
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            // Convert the map to a JSON string
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            e.printStackTrace(); // Handle the exception appropriately
            return null;
        }
    }
}


