package com.app.service.producer;

import com.app.model.StockData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;
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
        // Send data to Kafka
        kafkaStockProducerService.sendStockData(topicName, symbol, message);

    }

    private String convertRecordToJson(String line){
        String[] csvValues = line.split(",");
        JSONObject jsonObject = new JSONObject();
        try {
            jsonObject.put("timestamp", csvValues[0]);
            jsonObject.put("open", csvValues[1]);
            jsonObject.put("high", csvValues[2]);
            jsonObject.put("low", csvValues[3]);
            jsonObject.put("close", csvValues[4]);
            jsonObject.put("volume", csvValues[5]);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        // Convert the JSON object to a string
        return jsonObject.toString();

    }

    private String convertRecordToJsonOld(CSVRecord record) {
        try {
            // Create an ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            // Convert the CSVRecord to a Java object (you need to define a corresponding Java class)
            // Assuming you have a StockData class with fields like date, open, high, low, close, etc.
            StockData stockData = new StockData();
            stockData.setDate(record.get("Date"));
            stockData.setOpen(Double.parseDouble(record.get("Open")));
            stockData.setHigh(Double.parseDouble(record.get("High")));
            stockData.setLow(Double.parseDouble(record.get("Low")));
            stockData.setClose(Double.parseDouble(record.get("Close")));

            // Serialize the Java object to JSON
            String json = objectMapper.writeValueAsString(stockData);

            return json;
        } catch (Exception e) {
            // Handle any exceptions that may occur during conversion
            e.printStackTrace();
            return null;
        }
    }
}


