package com.leumanuel.woozydata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leumanuel.woozydata.model.DataFrame;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import java.io.*;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.stream.Collectors;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * @author Leu A. Manuel
 * * Service class for exporting analysis results in PowerBI-compatible format.
 * Handles export of all analysis results to a single Excel workbook.
 */
public class DataExportService {
    
    /**
     * Exports all analysis results to a single Excel file for PowerBI.
     * 
     * @param results Map of analysis results
     * @param filePath Output file path
     * @throws IOException if export fails
     */
    public void exportToPowerBI(Map<String, DataFrame> results, String filePath) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            for (Map.Entry<String, DataFrame> entry : results.entrySet()) {
                createSheet(workbook, entry.getKey(), entry.getValue());
            }
            
            try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
                workbook.write(fileOut);
            }
        }
    }    
    

    /**
     * Creates a standardized analysis report with multiple metrics.
     * 
     * @param df Source DataFrame
     * @return Map containing different analysis views
     */
    public Map<String, DataFrame> createAnalysisReport(DataFrame df) {
        Map<String, DataFrame> report = new HashMap<>();
        
        // Basic statistics
        report.put("basic_stats", calculateBasicStats(df));
        
        // Time series analysis (if applicable)
        if (hasTimeColumn(df)) {
            report.put("time_series", calculateTimeSeriesMetrics(df));
        }
        
        // Correlation analysis
        report.put("correlations", calculateCorrelations(df));
        
        // Aggregations
        report.put("aggregations", calculateAggregations(df));
        
        return report;
    }

    /**
     * Custom export with user-selected metrics.
     * 
     * @param df Source DataFrame
     * @param metrics List of metric names to export
     * @param filePath Output file path
     * @throws IOException if export fails
     */
    public void customExport(DataFrame df, List<String> metrics, String filePath) throws IOException {
        Map<String, DataFrame> selectedResults = new HashMap<>();
        
        metrics.forEach(metric -> {
            switch (metric) {
                case "basic_stats" -> selectedResults.put(metric, calculateBasicStats(df));
                case "time_series" -> selectedResults.put(metric, calculateTimeSeriesMetrics(df));
                case "correlations" -> selectedResults.put(metric, calculateCorrelations(df));
                case "aggregations" -> selectedResults.put(metric, calculateAggregations(df));
            }
        });
        
        exportToPowerBI(selectedResults, filePath);
    }

    // Private helper methods
    private void createSheet(Workbook workbook, String name, DataFrame df) {
        Sheet sheet = workbook.createSheet(name);
        
        // Create header row
        Row headerRow = sheet.createRow(0);
        List<String> columns = new ArrayList<>(df.getData().get(0).keySet());
        for (int i = 0; i < columns.size(); i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(columns.get(i));
        }
        
        // Create data rows
        for (int i = 0; i < df.getData().size(); i++) {
            Row row = sheet.createRow(i + 1);
            Map<String, Object> data = df.getData().get(i);
            for (int j = 0; j < columns.size(); j++) {
                Cell cell = row.createCell(j);
                setCellValue(cell, data.get(columns.get(j)));
            }
        }
        
        // Auto-size columns
        for (int i = 0; i < columns.size(); i++) {
            sheet.autoSizeColumn(i);
        }
    }

    private void setCellValue(Cell cell, Object value) {
        if (value == null) {
            cell.setBlank();
        } else if (value instanceof Number) {
            cell.setCellValue(((Number) value).doubleValue());
        } else if (value instanceof Boolean) {
            cell.setCellValue((Boolean) value);
        } else if (value instanceof LocalDateTime) {
            cell.setCellValue(((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        } else {
            cell.setCellValue(value.toString());
        }
    }

    private boolean hasTimeColumn(DataFrame df) {
        return df.getData().stream()
                .flatMap(row -> row.values().stream())
                .anyMatch(val -> val instanceof LocalDateTime);
    }

    
    /**
     * Calculates basic statistical measures for all numeric columns.
     * 
     * @param df Source DataFrame
     * @return DataFrame containing basic statistics for each numeric column
     */
    private DataFrame calculateBasicStats(DataFrame df) {
        List<Map<String, Object>> statsResult = new ArrayList<>();
        Set<String> numericColumns = getNumericColumns(df);

        for (String column : numericColumns) {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            df.getData().stream()
                .map(row -> row.get(column))
                .filter(val -> val instanceof Number)
                .forEach(val -> stats.addValue(((Number) val).doubleValue()));

            Map<String, Object> columnStats = new HashMap<>();
            columnStats.put("column", column);
            columnStats.put("count", stats.getN());
            columnStats.put("mean", stats.getMean());
            columnStats.put("median", stats.getPercentile(50));
            columnStats.put("std", stats.getStandardDeviation());
            columnStats.put("min", stats.getMin());
            columnStats.put("max", stats.getMax());
            columnStats.put("skewness", stats.getSkewness());
            columnStats.put("kurtosis", stats.getKurtosis());
            columnStats.put("variance", stats.getVariance());

            statsResult.add(columnStats);
        }

        return new DataFrame(statsResult);
    }

    /**
     * Calculates time series metrics including trends and seasonality.
     * 
     * @param df Source DataFrame
     * @return DataFrame containing time series analysis results
     */
    private DataFrame calculateTimeSeriesMetrics(DataFrame df) {
        List<Map<String, Object>> timeSeriesResult = new ArrayList<>();
        Set<String> numericColumns = getNumericColumns(df);

        for (String column : numericColumns) {
            // Calculate moving averages
            double[] values = df.getData().stream()
                .mapToDouble(row -> ((Number) row.get(column)).doubleValue())
                .toArray();

            int[] windows = {7, 14, 30}; // Different window sizes for analysis

            for (int window : windows) {
                Map<String, Object> metricRow = new HashMap<>();
                metricRow.put("column", column);
                metricRow.put("window_size", window);

                // Calculate moving average
                double[] ma = calculateMovingAverage(values, window);
                metricRow.put("moving_avg", Arrays.stream(ma).average().orElse(Double.NaN));

                // Calculate trend
                double[] trend = calculateTrend(values);
                metricRow.put("trend_slope", trend[0]);
                metricRow.put("trend_intercept", trend[1]);

                // Calculate seasonality
                double seasonalityIndex = calculateSeasonalityIndex(values, window);
                metricRow.put("seasonality_index", seasonalityIndex);

                timeSeriesResult.add(metricRow);
            }
        }

        return new DataFrame(timeSeriesResult);
    }

    /**
     * Calculates correlation matrix for all numeric columns.
     * 
     * @param df Source DataFrame
     * @return DataFrame containing correlation matrix
     */
    private DataFrame calculateCorrelations(DataFrame df) {
        List<Map<String, Object>> correlationResult = new ArrayList<>();
        Set<String> numericColumns = getNumericColumns(df);

        for (String col1 : numericColumns) {
            Map<String, Object> correlationRow = new HashMap<>();
            correlationRow.put("column", col1);

            for (String col2 : numericColumns) {
                double correlation = calculatePearsonCorrelation(
                    df.getData().stream()
                        .mapToDouble(row -> ((Number) row.get(col1)).doubleValue())
                        .toArray(),
                    df.getData().stream()
                        .mapToDouble(row -> ((Number) row.get(col2)).doubleValue())
                        .toArray()
                );
                correlationRow.put(col2, correlation);
            }

            correlationResult.add(correlationRow);
        }

        return new DataFrame(correlationResult);
    }

    /**
     * Calculates various aggregations for numeric columns.
     * 
     * @param df Source DataFrame
     * @return DataFrame containing aggregated results
     */
    private DataFrame calculateAggregations(DataFrame df) {
        List<Map<String, Object>> aggregationResult = new ArrayList<>();
        Set<String> numericColumns = getNumericColumns(df);

        // Get categorical columns for grouping
        Set<String> categoricalColumns = df.getData().get(0).keySet().stream()
            .filter(col -> !numericColumns.contains(col))
            .collect(Collectors.toSet());

        for (String groupCol : categoricalColumns) {
            // Group by categorical column
            Map<Object, List<Map<String, Object>>> grouped = df.getData().stream()
                .collect(Collectors.groupingBy(row -> row.get(groupCol)));

            // Calculate aggregations for each group
            for (Map.Entry<Object, List<Map<String, Object>>> entry : grouped.entrySet()) {
                Map<String, Object> aggRow = new HashMap<>();
                aggRow.put("group_column", groupCol);
                aggRow.put("group_value", entry.getKey());

                // Calculate aggregations for each numeric column
                for (String numCol : numericColumns) {
                    DescriptiveStatistics stats = new DescriptiveStatistics();
                    entry.getValue().stream()
                        .map(row -> row.get(numCol))
                        .filter(val -> val instanceof Number)
                        .forEach(val -> stats.addValue(((Number) val).doubleValue()));

                    aggRow.put(numCol + "_mean", stats.getMean());
                    aggRow.put(numCol + "_sum", stats.getSum());
                    aggRow.put(numCol + "_count", stats.getN());
                    aggRow.put(numCol + "_std", stats.getStandardDeviation());
                }

                aggregationResult.add(aggRow);
            }
        }

        return new DataFrame(aggregationResult);
    }

    // Helper methods
    private Set<String> getNumericColumns(DataFrame df) {
        return df.getData().get(0).entrySet().stream()
            .filter(e -> e.getValue() instanceof Number)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    private double[] calculateMovingAverage(double[] values, int window) {
        double[] result = new double[values.length - window + 1];
        for (int i = 0; i < result.length; i++) {
            double sum = 0;
            for (int j = 0; j < window; j++) {
                sum += values[i + j];
            }
            result[i] = sum / window;
        }
        return result;
    }

    private double[] calculateTrend(double[] values) {
        int n = values.length;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;

        for (int i = 0; i < n; i++) {
            sumX += i;
            sumY += values[i];
            sumXY += i * values[i];
            sumX2 += i * i;
        }

        double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        double intercept = (sumY - slope * sumX) / n;

        return new double[]{slope, intercept};
    }

    private double calculateSeasonalityIndex(double[] values, int period) {
        double[] detrended = new double[values.length];
        double[] trend = calculateTrend(values);

        // Remove trend
        for (int i = 0; i < values.length; i++) {
            detrended[i] = values[i] - (trend[0] * i + trend[1]);
        }

        // Calculate seasonality index
        double sumSquares = 0;
        for (int i = 0; i < detrended.length - period; i++) {
            double diff = detrended[i + period] - detrended[i];
            sumSquares += diff * diff;
        }

        return Math.sqrt(sumSquares / (detrended.length - period));
    }

    private double calculatePearsonCorrelation(double[] x, double[] y) {
        DescriptiveStatistics statsX = new DescriptiveStatistics(x);
        DescriptiveStatistics statsY = new DescriptiveStatistics(y);

        double meanX = statsX.getMean();
        double meanY = statsY.getMean();
        double stdX = statsX.getStandardDeviation();
        double stdY = statsY.getStandardDeviation();

        double sum = 0;
        for (int i = 0; i < x.length; i++) {
            sum += (x[i] - meanX) * (y[i] - meanY);
        }

        return sum / (x.length * stdX * stdY);
    }
    
     /**
     * Exports DataFrame content to a CSV (Comma-Separated Values) file.
     * The first row contains headers, and subsequent rows contain data values.
     * Values are separated by commas, and special characters are properly escaped.
     *
     * @param df DataFrame to be exported
     * @param filePath Path where the CSV file will be saved
     * @throws IOException if there is an error writing to the file
     */
    public void exportToCSV(DataFrame df, String filePath) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
            // Write headers
            Set<String> headers = df.getData().get(0).keySet();
            writer.println(String.join(",", headers));
            
            // Write data
            for (Map<String, Object> row : df.getData()) {
                String line = headers.stream()
                    .map(header -> formatValue(row.get(header)))
                    .collect(Collectors.joining(","));
                writer.println(line);
            }
        }
    }
    
    /**
     * Exports DataFrame content to a JSON (JavaScript Object Notation) file.
     * The data is formatted as a JSON array of objects with pretty printing
     * enabled for better readability.
     *
     * @param df DataFrame to be exported
     * @param filePath Path where the JSON file will be saved
     * @throws IOException if there is an error writing to the file
     */
    public void exportToJSON(DataFrame df, String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter()
              .writeValue(new File(filePath), df.getData());
    }
    
    /**
     * Exports DataFrame content to a Excel file.
     * The data is formatted as a Excel
     * enabled for better readability.
     *
     * @param df DataFrame to be exported
     * @param filePath Path where the Excel file will be saved
     * @throws IOException if there is an error writing to the file
     */
    public void exportToExcel(DataFrame df, String filePath) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet("Data");
            
            // Create header row
            Row headerRow = sheet.createRow(0);
            List<String> headers = new ArrayList<>(df.getData().get(0).keySet());
            for (int i = 0; i < headers.size(); i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers.get(i));
            }
            
            // Create data rows
            for (int i = 0; i < df.getData().size(); i++) {
                Row row = sheet.createRow(i + 1);
                Map<String, Object> data = df.getData().get(i);
                for (int j = 0; j < headers.size(); j++) {
                    Cell cell = row.createCell(j);
                    setCellValue(cell, data.get(headers.get(j)));
                }
            }
            
            // Auto-size columns
            for (int i = 0; i < headers.size(); i++) {
                sheet.autoSizeColumn(i);
            }
            
            // Write to file
            try (FileOutputStream fileOut = new FileOutputStream(filePath)) {
                workbook.write(fileOut);
            }
        }
    }
    
    
     /**
     * Exports DataFrame content to a HTML file.
     * The data is formatted as a HTML
     * enabled for better readability.
     *
     * @param df DataFrame to be exported
     * @param filePath Path where the HTML file will be saved
     * @throws IOException if there is an error writing to the file
     */   
    public void exportToHTML(DataFrame df, String filePath) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
            writer.println("<html><body>");
            writer.println("<table border='1'>");
            
            // Write headers
            writer.println("<tr>");
            df.getData().get(0).keySet().forEach(header -> 
                writer.printf("<th>%s</th>", header));
            writer.println("</tr>");
            
            // Write data
            for (Map<String, Object> row : df.getData()) {
                writer.println("<tr>");
                row.values().forEach(value -> 
                    writer.printf("<td>%s</td>", formatValue(value)));
                writer.println("</tr>");
            }
            
            writer.println("</table>");
            writer.println("</body></html>");
        }
    }
    
        /**
     * Exports DataFrame content to a Latex file.
     * The data is formatted as a Latex
     * enabled for better readability.
     *
     * @param df DataFrame to be exported
     * @param filePath Path where the Latex file will be saved
     * @throws IOException if there is an error writing to the file
     */ 
    public void exportToLatex(DataFrame df, String filePath) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(filePath))) {
            List<String> headers = new ArrayList<>(df.getData().get(0).keySet());
            
            // Begin LaTeX table
            writer.println("\\begin{table}[h!]");
            writer.println("\\begin{tabular}{" + "l".repeat(headers.size()) + "}");
            writer.println("\\hline");
            
            // Write headers
            writer.println(headers.stream()
                .map(this::escapeLatex)
                .collect(Collectors.joining(" & ")) + " \\\\");
            writer.println("\\hline");
            
            // Write data
            for (Map<String, Object> row : df.getData()) {
                writer.println(headers.stream()
                    .map(h -> escapeLatex(formatValue(row.get(h))))
                    .collect(Collectors.joining(" & ")) + " \\\\");
            }
            
            // End LaTeX table
            writer.println("\\hline");
            writer.println("\\end{tabular}");
            writer.println("\\end{table}");
        }
    }
      
    private String formatValue(Object value) {
        if (value == null) return "";
        return value.toString().replace(",", ";");
    }
    
    private String escapeLatex(String text) {
        return text.replace("&", "\\&")
                  .replace("%", "\\%")
                  .replace("$", "\\$")
                  .replace("#", "\\#")
                  .replace("_", "\\_")
                  .replace("{", "\\{")
                  .replace("}", "\\}")
                  .replace("~", "\\~")
                  .replace("^", "\\^");
    }

}
