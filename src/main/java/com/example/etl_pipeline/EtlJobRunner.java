package com.example.etl_pipeline;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class EtlJobRunner implements CommandLineRunner {

    private final FinancialDataService financialDataService;

    public EtlJobRunner(FinancialDataService financialDataService) {
        this.financialDataService = financialDataService;
    }

    @Override
    public void run(String... args) throws Exception {
        List<String> symbols = Arrays.asList("AAPL", "MSFT", "GOOGL");
        System.out.println("Starting ETL job for symbols: " + symbols);
        
        for (String symbol : symbols) {
            financialDataService.fetchAndSaveData(symbol);
        }

        System.out.println("ETL job finished.");
    }
}