package com.app.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@Service
public class StockDataService {

    @Autowired
    private AlphaVantageService alphaVantageService;

    @Autowired
    private KafkaStockProducerService kafkaStockProducerService;

    public Map<String, Object> processAndSendStockData(String symbol) {
        // Fetch data
        Map<String, Object> stockData = alphaVantageService.getDailyTimeSeries(symbol);

        // Process data (optional, depending on your use case)
        String processedData = processStockData(stockData);

        // Send data to Kafka
        kafkaStockProducerService.sendStockData(symbol, processedData);

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


