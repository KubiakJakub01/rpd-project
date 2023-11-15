package com.app.service;

import com.app.service.producer.KafkaStockProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;
import org.springframework.stereotype.Service;


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

}


