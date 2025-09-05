package com.example.etl_pipeline;

import java.time.LocalDate;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import lombok.Data;
import jakarta.persistence.*;

@Data
@Entity
@Table(name = "daily_stock_prices")
public class StockPrice {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String symbol;
    private LocalDate date;
    private double open;
    private double high;
    private double low;
    private double close;
    private long volume;
}
