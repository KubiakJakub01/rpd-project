package com.app.controller;

import com.app.service.StockDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class StockDataController {

    @Autowired
    private StockDataService stockDataService;

    @GetMapping("/fetchAndSendStockData/{symbol}")
    public ResponseEntity<Map<String, Object>> fetchAndSendStockData(@PathVariable String symbol) {
        Map<String, Object> stockData = stockDataService.processAndSendStockData(symbol);
        return ResponseEntity.ok(stockData);
    }
}


