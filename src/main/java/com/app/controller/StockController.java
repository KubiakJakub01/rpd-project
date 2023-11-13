package com.app.controller;

import com.app.service.AlphaVantageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
public class StockController {

    @Autowired
    private AlphaVantageService alphaVantageService;

    @GetMapping("/stock/daily/{symbol}")
    public Map<String, Object> getDailyTimeSeries(@PathVariable String symbol) {
        return alphaVantageService.getDailyTimeSeries(symbol);
    }
}


