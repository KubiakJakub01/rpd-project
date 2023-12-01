package com.app.scheduler;

import com.app.service.MovieDataService;
import com.app.service.StockDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
/*
@Component
public class MovieDataScheduler {

    @Autowired
    private MovieDataService movieDataService;

    private LocalDate currentDate = LocalDate.now();

    @Scheduled(fixedRate = 30000) // Runs every x seconds
    public void scheduleStockDataProcessing() {
        String movie = "a"; // example movie symbol

        // Format the current date to get the year and month
        String year = currentDate.format(DateTimeFormatter.ofPattern("yyyy"));

        // Process and send stock data for the current month
        movieDataService.processAndSendMovieData(movie, year);

        // Update the currentDate to fetch data from the previous month for the next call
        currentDate = currentDate.minusYears(1);
    }

}

 */


