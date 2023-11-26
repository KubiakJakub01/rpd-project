package com.app.scheduler;

import com.app.service.producer.CsvKafkaProducer;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class CsvBatchReaderScheduler {

    @Autowired
    private CsvKafkaProducer csvKafkaProducer;

    @Autowired
    private ResourceLoader resourceLoader;

    private int lastProcessedLine = 0;

    @Scheduled(fixedRate = 30000) // Runs every minute
    public void processCsvBatch() {
        String resourcePath = "classpath:static/weekly_IBM.csv";
        String symbol = extractSymbolFromFilename(resourcePath);
        Resource resource = resourceLoader.getResource(resourcePath);

        try (InputStream is = resource.getInputStream();
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            List<String> allLines = reader.lines().collect(Collectors.toList());

            // Skip the header for processing
            if (lastProcessedLine == 0) {
                lastProcessedLine = 1; // Skip the first line which is the header
            }
            int toLine = Math.min(allLines.size(), lastProcessedLine + BATCH_SIZE);

            for (int i = lastProcessedLine; i < toLine; i++) {
                String line = allLines.get(i);
                // Process and send line to Kafka
                csvKafkaProducer.sendCsvLineToKafka(symbol, line);
            }

            lastProcessedLine = toLine; // Update the last processed line
        } catch (IOException e) {
            e.printStackTrace(); // Handle exceptions
        }
    }

    private static final int BATCH_SIZE = 100; // Number of lines to process in each batch

    private String extractSymbolFromFilename(String resourcePath) {
        // Extract the file name from the resource path
        String fileName = resourcePath.substring(resourcePath.lastIndexOf("/") + 1);

        // Split the filename using underscores as separators
        String[] parts = fileName.split("_");

        // Check if there are at least one part in the filename
        if (parts.length > 0) {
            // The symbol is the first part of the filename (assuming format is symbol_something.csv)
            return parts[0];
        } else {
            // Return an empty string or handle the error as needed
            return "";
        }
    }

}

