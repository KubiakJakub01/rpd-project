package com.app.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.util.Map;

@Service
public class AlphaVantageService {

    @Value("${alphavantage.apikey}")
    private String apiKey;

    private final RestTemplate restTemplate;

    public AlphaVantageService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public Map<String, Object> getDailyTimeSeries(String symbol) {
        String url = String.format(
                "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&apikey=%s",
                symbol, apiKey);
        return restTemplate.getForObject(url, Map.class);
    }

    public Map<String, Object> getDailyTimeSeries(String symbol,String month) {
        String url = String.format(
                "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&apikey=%s&" +
                        "interval=60min&month=%s",
                symbol, apiKey,month);
        return restTemplate.getForObject(url, Map.class);
    }

}


