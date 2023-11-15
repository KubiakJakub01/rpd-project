package com.app.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class MovieDataService {

    @Autowired
    private OmdbService omdbService;

    @Autowired
    private KafkaStockProducerService kafkaStockProducerService;

    // overloaded method for writing to kafka topic with month
    public Map<String, Object> processAndSendMovieData(String movie,String year) {
        // Fetch data
        Map<String, Object> movieData = omdbService.getMovieOfTheYear(movie,year);

        // Process data (optional, depending on your use case)
        String processedData = processMovieData(movieData);

        System.out.println("Sending message to Kafka: " + processedData);

        // Send data to Kafka
        kafkaStockProducerService.sendStockData(movie, processedData);

        // Return the fetched data
        return movieData;
    }

    private String processMovieData(Map<String, Object> stockData) {
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


