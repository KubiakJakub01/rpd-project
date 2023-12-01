package com.app.model;

import lombok.Data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Data
public class StockData {
    private Date timestamp;
    private double open;
    private double high;
    private double low;
    private double close;
    private int volume;

    public void setDate(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            // Parse the date string into a Date object
            this.timestamp = dateFormat.parse(date);
        } catch (ParseException e) {
            // Handle parsing errors, e.g., invalid date format
            e.printStackTrace();
            // You might want to throw an exception or handle the error differently here
        }
    }
}
