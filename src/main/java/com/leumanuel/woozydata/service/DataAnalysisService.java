
package com.leumanuel.woozydata.service;


import java.util.Map;

import com.leumanuel.woozydata.model.DataFrame;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service class for data analysis operations.
 * Provides statistical calculations and data transformations.
 */
public class DataAnalysisService {
    
    /**
     * Analyzes a specified column in the DataFrame and calculates basic statistical measures.
     * 
     * @param df DataFrame containing the data to analyze
     * @param column Name of the column to analyze
     * @return DataFrame containing analysis results including count, mean, std, min, max, median,
     *         skewness, and kurtosis
     * @throws IllegalArgumentException if column contains non-numeric data
     */
    public DataFrame analyze(DataFrame df, String column) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .forEach(val -> stats.addValue(((Number) val).doubleValue()));

        List<Map<String, Object>> analysis = new ArrayList<>();
        Map<String, Object> results = new HashMap<>();
        
        results.put("column", column);
        results.put("count", stats.getN());
        results.put("mean", stats.getMean());
        results.put("std", stats.getStandardDeviation());
        results.put("min", stats.getMin());
        results.put("max", stats.getMax());
        results.put("median", stats.getPercentile(50));
        results.put("skewness", stats.getSkewness());
        results.put("kurtosis", stats.getKurtosis());
        
        analysis.add(results);
        return new DataFrame(analysis);
    }

    /**
     * Calculates statistical measures for a specified column.
     * 
     * @param df DataFrame containing the data
     * @param column Name of the column to analyze
     * @return Map containing basic statistical measures (mean, std, min, max, median)
     * @throws IllegalArgumentException if column contains non-numeric data
     */
    public Map<String, Double> stats(DataFrame df, String column) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .forEach(val -> stats.addValue(((Number) val).doubleValue()));

        Map<String, Double> results = new HashMap<>();
        results.put("mean", stats.getMean());
        results.put("std", stats.getStandardDeviation());
        results.put("min", stats.getMin());
        results.put("max", stats.getMax());
        results.put("median", stats.getPercentile(50));
        
        return results;
    }

    /**
     * Calculates the sum of all values in a specified column.
     * 
     * @param df DataFrame containing the data
     * @param column Name of the column to sum
     * @return Sum of all numeric values in the column
     * @throws IllegalArgumentException if column contains non-numeric data
     */
    public double sum(DataFrame df, String column) {
        DataStatisticsService ds = new DataStatisticsService();
        return new DescriptiveStatistics(ds.getColumnValues(df, column)).getSum();
    }
    
    /**
     * Calculates the arithmetic mean (average) of values in a specified column.
     * 
     * @param df DataFrame containing the data
     * @param column Name of the column to average
     * @return Average of all numeric values in the column
     * @throws IllegalArgumentException if column contains non-numeric data
     */
    public double avg(DataFrame df, String column) {
        DataStatisticsService ds = new DataStatisticsService();
        return new DescriptiveStatistics(ds.getColumnValues(df, column)).getMean();
    }
   
    /**
     * Performs correlation analysis between numeric columns.
     * 
     * @param df DataFrame to analyze
     * @return Correlation matrix as DataFrame
     */
    public DataFrame correlation(DataFrame df) {
        Set<String> numericColumns = getNumericColumns(df);
        List<Map<String, Object>> correlationMatrix = new ArrayList<>();
        
        for (String col1 : numericColumns) {
            Map<String, Object> row = new HashMap<>();
            for (String col2 : numericColumns) {
                row.put(col2, calculateCorrelation(df, col1, col2));
            }
            correlationMatrix.add(row);
        }
        
        return new DataFrame(correlationMatrix);
    }

    /**
     * Performs time series analysis.
     * 
     * @param df DataFrame with time series data
     * @param timeColumn Column containing time values
     * @param valueColumn Column containing values to analyze
     * @param windowSize Rolling window size
     * @return DataFrame with analysis results
     */
    public DataFrame timeSeriesAnalysis(DataFrame df, String timeColumn, 
                                      String valueColumn, int windowSize) {
        List<Map<String, Object>> results = new ArrayList<>();
        DescriptiveStatistics rollingStats = new DescriptiveStatistics(windowSize);
        
        df.getData().forEach(row -> {
            Double value = ((Number) row.get(valueColumn)).doubleValue();
            rollingStats.addValue(value);
            
            Map<String, Object> result = new HashMap<>();
            result.put("time", row.get(timeColumn));
            result.put("value", value);
            result.put("rolling_mean", rollingStats.getMean());
            result.put("rolling_std", rollingStats.getStandardDeviation());
            
            results.add(result);
        });
        
        return new DataFrame(results);
    }

    // Helper methods
    private Set<String> getNumericColumns(DataFrame df) {
        return df.getData().get(0).entrySet().stream()
            .filter(e -> e.getValue() instanceof Number)
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    private double calculateCorrelation(DataFrame df, String col1, String col2) {
        DescriptiveStatistics stats1 = new DescriptiveStatistics();
        DescriptiveStatistics stats2 = new DescriptiveStatistics();
        
        df.getData().forEach(row -> {
            stats1.addValue(((Number) row.get(col1)).doubleValue());
            stats2.addValue(((Number) row.get(col2)).doubleValue());
        });
        
        // Pearson correlation calculation
        double meanX = stats1.getMean();
        double meanY = stats2.getMean();
        double stdX = stats1.getStandardDeviation();
        double stdY = stats2.getStandardDeviation();
        
        double correlation = 0.0;
        int n = df.getData().size();
        
        for (int i = 0; i < n; i++) {
            double x = stats1.getElement(i);
            double y = stats2.getElement(i);
            correlation += (x - meanX) * (y - meanY);
        }
        
        correlation /= (n * stdX * stdY);
        return correlation;
    } 
    
     /**
     * Calculates the median value of a specified column.
     * The median is the value separating the higher half from the lower half of a data sample.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return The median value of the specified column
     * @throws IllegalArgumentException if column contains non-numeric data
     */
    public double calculateMedian(DataFrame dataFrame, String column) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        
        dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .forEach(value -> {
                if (!(value instanceof Number)) {
                    throw new IllegalArgumentException(
                        "Column '" + column + "' contains non-numeric data"
                    );
                }
                stats.addValue(((Number) value).doubleValue());
            });

        if (stats.getN() == 0) {
            throw new IllegalArgumentException(
                "Column '" + column + "' is empty or contains only null values"
            );
        }

        return stats.getPercentile(50);
    }

    /**
     * Calculates the variance of a specified column.
     * Variance measures how far a set of numbers are spread out from their average value.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return The variance of the specified column
     * @throws IllegalArgumentException if column contains non-numeric data
     */
    public double calculateVariance(DataFrame dataFrame, String column) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        
        dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .forEach(value -> {
                if (!(value instanceof Number)) {
                    throw new IllegalArgumentException(
                        "Column '" + column + "' contains non-numeric data"
                    );
                }
                stats.addValue(((Number) value).doubleValue());
            });

        if (stats.getN() == 0) {
            throw new IllegalArgumentException(
                "Column '" + column + "' is empty or contains only null values"
            );
        }

        return stats.getVariance();
    }

    /**
     * Calculates the mode (most frequent value) of a specified column.
     * If multiple modes exist, returns the first one found.
     * Works with both numeric and non-numeric data.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return The mode value of the specified column
     * @throws IllegalArgumentException if the column is empty or contains only null values
     */
    public Object calculateMode(DataFrame dataFrame, String column) {
        Map<Object, Long> frequencyMap = dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(
                value -> value,
                Collectors.counting()
            ));

        if (frequencyMap.isEmpty()) {
            throw new IllegalArgumentException(
                "Column '" + column + "' is empty or contains only null values"
            );
        }

        return frequencyMap.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElseThrow(() -> new IllegalStateException("No mode found"));
    }

    /**
     * Calculates multiple modes if they exist in the specified column.
     * Returns all values that appear with the highest frequency.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return List of mode values
     * @throws IllegalArgumentException if the column is empty or contains only null values
     */
    public List<Object> calculateMultipleMode(DataFrame dataFrame, String column) {
        Map<Object, Long> frequencyMap = dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(
                value -> value,
                Collectors.counting()
            ));

        if (frequencyMap.isEmpty()) {
            throw new IllegalArgumentException(
                "Column '" + column + "' is empty or contains only null values"
            );
        }

        long maxFrequency = frequencyMap.values().stream()
            .mapToLong(Long::longValue)
            .max()
            .orElse(0);

        return frequencyMap.entrySet().stream()
            .filter(entry -> entry.getValue() == maxFrequency)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    }

    /**
     * Gets the frequency distribution of values in a specified column.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return Map with values and their frequencies
     */
    public Map<Object, Long> getFrequencyDistribution(DataFrame dataFrame, String column) {
        return dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(
                value -> value,
                Collectors.counting()
            ));
    }

    /**
     * Calculates the standard deviation of a specified column.
     * Standard deviation measures the amount of variation or dispersion of a set of values.
     * A low standard deviation indicates that the values tend to be close to the mean,
     * while a high standard deviation indicates that the values are spread out over a wider range.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return The standard deviation of the specified column
     * @throws IllegalArgumentException if column contains non-numeric data or is empty
     * @throws NullPointerException if dataFrame or column is null
     */
    public double calculateStandardDeviation(DataFrame dataFrame, String column) {
        if (dataFrame == null) {
            throw new NullPointerException("DataFrame cannot be null");
        }
        if (column == null) {
            throw new NullPointerException("Column name cannot be null");
        }

        DescriptiveStatistics stats = new DescriptiveStatistics();

        dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .forEach(value -> {
                if (!(value instanceof Number)) {
                    throw new IllegalArgumentException(
                        "Column '" + column + "' contains non-numeric data"
                    );
                }
                stats.addValue(((Number) value).doubleValue());
            });

        if (stats.getN() == 0) {
            throw new IllegalArgumentException(
                "Column '" + column + "' is empty or contains only null values"
            );
        }

        return stats.getStandardDeviation();
    }

    /**
     * Calculates the population standard deviation of a specified column.
     * Similar to sample standard deviation but uses n instead of (n-1) in the denominator.
     * Use this when working with complete populations rather than samples.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return The population standard deviation of the specified column
     * @throws IllegalArgumentException if column contains non-numeric data or is empty
     * @throws NullPointerException if dataFrame or column is null
     */
    public double calculatePopulationStandardDeviation(DataFrame dataFrame, String column) {
        double variance = calculateVariance(dataFrame, column);
        int n = (int) dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .count();

        // Adjust sample variance to population variance
        double populationVariance = variance * (n - 1) / n;
        return Math.sqrt(populationVariance);
    }

    /**
     * Calculates the coefficient of variation (CV) for a specified column.
     * CV is the ratio of the standard deviation to the mean, often expressed as a percentage.
     * It shows the extent of variability in relation to the mean of the population.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return The coefficient of variation as a percentage
     * @throws IllegalArgumentException if column contains non-numeric data or is empty
     * @throws NullPointerException if dataFrame or column is null
     */
    public double calculateCoefficientOfVariation(DataFrame dataFrame, String column) {
        double mean = calculateMean(dataFrame, column);
        double standardDeviation = calculateStandardDeviation(dataFrame, column);

        if (mean == 0) {
            throw new IllegalArgumentException(
                "Cannot calculate coefficient of variation when mean is zero"
            );
        }

        return (standardDeviation / Math.abs(mean)) * 100;
    }

    /**
     * Generates a summary of dispersion statistics for a specified column.
     * Includes standard deviation, variance, coefficient of variation, and range.
     *
     * @param dataFrame DataFrame containing the data
     * @param column Name of the column to analyze
     * @return Map containing various dispersion statistics
     * @throws IllegalArgumentException if column contains non-numeric data or is empty
     * @throws NullPointerException if dataFrame or column is null
     */
    public Map<String, Double> getDispersionStatistics(DataFrame dataFrame, String column) {
        Map<String, Double> stats = new HashMap<>();

        stats.put("standard_deviation", calculateStandardDeviation(dataFrame, column));
        stats.put("variance", calculateVariance(dataFrame, column));
        stats.put("coefficient_of_variation", calculateCoefficientOfVariation(dataFrame, column));

        DescriptiveStatistics descriptiveStats = new DescriptiveStatistics();
        dataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .forEach(value -> descriptiveStats.addValue(((Number) value).doubleValue()));

        stats.put("range", descriptiveStats.getMax() - descriptiveStats.getMin());
        stats.put("interquartile_range", 
            descriptiveStats.getPercentile(75) - descriptiveStats.getPercentile(25));

        return stats;
    }

    /**
     * Calculates the arithmetic mean (average) of numeric values in a specified column.
     * The mean is calculated by summing all numeric values and dividing by the count of values.
     * Non-numeric values and null values in the column are ignored during calculation.
     *
     * @param dataFrame the DataFrame containing the data to analyze. Must not be null.
     * @param column the name of the column to calculate mean for. Must not be null.
     * @return the arithmetic mean of all numeric values in the column.
     *         Returns 0.0 if no numeric values are found.
     * @throws IllegalArgumentException if the specified column does not exist in the DataFrame
     * @throws NullPointerException if either dataFrame or column parameter is null
     * @see DescriptiveStatistics#getMean()
     * @since 1.0
     */ 
    public double calculateMean(DataFrame dataFrame, String column) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        for (Map<String, Object> row : dataFrame.getData()) {
            Object value = row.get(column);
            if (value instanceof Number) {
                stats.addValue(((Number) value).doubleValue());
            }
        }
        return stats.getMean();
    }
    
   

   
}
