package com.example.etl_pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class FinancialDataService {

    @Value("${alphavantage.api.key}")
    private String apiKey;

    @Value("${alphavantage.api.url}")
    private String apiUrl;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final StockPriceRepository stockPriceRepository;

    public FinancialDataService(StockPriceRepository stockPriceRepository) {
        this.stockPriceRepository = stockPriceRepository;
    }

    public void fetchAndSaveData(String symbol) {
        System.out.println("Fetching data for symbol: " + symbol);
        String url = String.format("%s?function=TIME_SERIES_DAILY&symbol=%s&apikey=%s", apiUrl, symbol, apiKey);

        try {
            String jsonResponse = restTemplate.getForObject(url, String.class);
            
            // --- NEW LOGGING 1: Print the raw JSON response ---
            System.out.println("RAW JSON RESPONSE for " + symbol + ":\n" + jsonResponse);

            JsonNode root = objectMapper.readTree(jsonResponse);
            JsonNode timeSeries = root.path("Time Series (Daily)");

            // --- NEW LOGGING 2: Check if the main data node exists ---
            if (timeSeries.isMissingNode() || timeSeries.size() == 0) {
                System.err.println("ERROR: Could not find 'Time Series (Daily)' node or it is empty in the JSON response for " + symbol);
                // Also check for an error message from the API
                if (root.has("Error Message")) {
                    System.err.println("API Error Message: " + root.get("Error Message").asText());
                }
                 if (root.has("Note")) {
                    System.err.println("API Note: " + root.get("Note").asText());
                }
                return; // Stop processing for this symbol
            }


            List<StockPrice> stockPrices = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> fields = timeSeries.fields();

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                LocalDate date = LocalDate.parse(entry.getKey(), DateTimeFormatter.ISO_LOCAL_DATE);
                JsonNode priceData = entry.getValue();

                StockPrice stockPrice = new StockPrice();
                stockPrice.setSymbol(symbol);
                stockPrice.setDate(date);
                stockPrice.setOpen(priceData.path("1. open").asDouble());
                stockPrice.setHigh(priceData.path("2. high").asDouble());
                stockPrice.setLow(priceData.path("3. low").asDouble());
                stockPrice.setClose(priceData.path("4. close").asDouble());
                stockPrice.setVolume(priceData.path("5. volume").asLong());

                stockPrices.add(stockPrice);
            }

            // --- NEW LOGGING 3: Check how many records we are about to save ---
            System.out.println("PARSED " + stockPrices.size() + " records for symbol: " + symbol);

            if (!stockPrices.isEmpty()) {
                stockPriceRepository.saveAll(stockPrices);
                System.out.println("Successfully SAVED " + stockPrices.size() + " records for symbol: " + symbol);
            } else {
                 System.err.println("WARNING: No records were parsed. Nothing to save for symbol: " + symbol);
            }


        } catch (Exception e) {
             // --- NEW LOGGING 4: Make sure we see any exception clearly ---
            System.err.println("CRITICAL ERROR fetching or saving data for symbol " + symbol);
            e.printStackTrace();
        }
    }

}