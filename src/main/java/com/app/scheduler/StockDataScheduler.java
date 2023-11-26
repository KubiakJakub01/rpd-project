package com.app.scheduler;

import com.app.service.StockDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
public class StockDataScheduler {

    @Autowired
    private StockDataService stockDataService;

    private LocalDate currentDate = LocalDate.now();

    @Scheduled(fixedRate = 30000) // Runs every x seconds
    public void scheduleStockDataProcessing() {
        String symbol = "IBM";

        // Format the current date to get the year and month
        String formattedDate = currentDate.format(DateTimeFormatter.ofPattern("yyyy-MM"));

        // Process and send stock data for the current month
        stockDataService.processAndSendStockData(symbol, formattedDate);

        // Update the currentDate to fetch data from the previous month for the next call
        currentDate = currentDate.minusMonths(1);
    }

}


