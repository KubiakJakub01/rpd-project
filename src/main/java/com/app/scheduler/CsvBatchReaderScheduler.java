package com.app.scheduler;

import com.app.service.producer.CsvKafkaProducer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Component
public class CsvBatchReaderScheduler {

    @Autowired
    private CsvKafkaProducer csvKafkaProducer;

    private int lastProcessedLine = 0;

    @Scheduled(fixedRate = 60000) // Runs every minute
    public void processCsvBatch() {
        String filePath = "src/main/resources/static/intraday_1min_IBM.csv"; // Set your CSV file path
        String symbol = extractSymbolFromFilename(filePath);

        try {
            List<String> allLines = Files.readAllLines(Paths.get(filePath));
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
            // Handle exception
        }
    }

    private static final int BATCH_SIZE = 100; // Number of lines to process in each batch

    private String extractSymbolFromFilename(String filePath) {
        // Split the filename using underscores as separators
        String[] parts = filePath.split("_");

        // Check if there are at least one part in the filename
        if (parts.length > 0) {
            // The symbol is the last part of the filename
            return parts[parts.length - 1].replaceFirst("\\.csv$", "");
        } else {
            // Return an empty string or handle the error as needed
            return "";
        }
    }
}

