package com.app.config;

import com.app.service.StockDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class StockDataScheduler {

    @Autowired
    private StockDataService stockDataService;

    @Scheduled(fixedRate = 60000) // Runs every 60 seconds
    public void scheduleStockDataProcessing() {
        String symbol = "AAPL"; // example stock symbol
        stockDataService.processAndSendStockData(symbol);
    }
}

