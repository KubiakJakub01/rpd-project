package com.app.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Service
public class OmdbService {

    @Value("${omdb.apikey}")
    private String apiKey;

    private final RestTemplate restTemplate;

    public OmdbService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public Map<String, Object> getMovieOfTheYear(String movie,String year) {
        String url = String.format(
                "https://www.omdbapi.com/?t=%s&apikey=%s&y=%s",
                movie, apiKey,year);
        return restTemplate.getForObject(url, Map.class);
    }

}


