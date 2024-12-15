package com.leumanuel.woozydata.service;

import com.leumanuel.woozydata.model.DataFrame;
import java.time.Year;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.distribution.NormalDistribution;

/**
 * Service class that provides advanced statistical analysis capabilities for DataFrames.
 * This class includes methods for time series analysis, outlier detection, missing value analysis,
 * and comprehensive statistical reporting.
 */

public class AdvancedAnalysisService {
     /**
     * Performs time series analysis on a DataFrame with date and value columns.
     * 
     * @param df The DataFrame containing the time series data
     * @param dateCol The name of the column containing date/time values
     * @param valueCol The name of the column containing numeric values to analyze
     * @return DataFrame containing time series analysis results including trends, autocorrelation,
     *         and basic statistics
     */
    public DataFrame timeAnalysis(DataFrame df, String dateCol, String valueCol) {
        List<Map<String, Object>> analysis = new ArrayList<>();

        List<Map<String, Object>> sortedData = df.getData().stream()
            .sorted(Comparator.comparing(row -> row.get(dateCol).toString()))
            .collect(Collectors.toList());

 
        double[] values = sortedData.stream()
            .map(row -> row.get(valueCol))
            .filter(val -> val instanceof Number)
            .mapToDouble(val -> ((Number) val).doubleValue())
            .toArray();

     
        DescriptiveStatistics stats = new DescriptiveStatistics(values);
        
       
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < values.length; i++) {
            regression.addData(i, values[i]);
        }

      
        PearsonsCorrelation correlation = new PearsonsCorrelation();
        double[] laggedValues = Arrays.copyOfRange(values, 1, values.length);
        double[] originalValues = Arrays.copyOfRange(values, 0, values.length - 1);
        double autocorrelation = correlation.correlation(originalValues, laggedValues);

        Map<String, Object> result = new HashMap<>();
        result.put("mean", stats.getMean());
        result.put("std", stats.getStandardDeviation());
        result.put("trend_slope", regression.getSlope());
        result.put("trend_intercept", regression.getIntercept());
        result.put("autocorrelation", autocorrelation);
        result.put("min", stats.getMin());
        result.put("max", stats.getMax());
        result.put("data_points", values.length);
        
        analysis.add(result);
        return new DataFrame(analysis);
    }

     /**
     * Analyzes outliers in specified numeric columns using both Z-score and IQR methods.
     * 
     * @param df The DataFrame to analyze
     * @param columns The column names to check for outliers
     * @return DataFrame containing outlier analysis results for each column including
     *         outlier counts, bounds, and detailed outlier information
     */
    public DataFrame outlierAnalysis(DataFrame df, String... columns) {
        List<Map<String, Object>> analysis = new ArrayList<>();
        
        for (String column : columns) {
            if (!isNumericColumn(df, column)) continue;
            
            double[] values = getColumnValues(df, column);
            DescriptiveStatistics stats = new DescriptiveStatistics(values);
            
            double mean = stats.getMean();
            double std = stats.getStandardDeviation();
            double q1 = stats.getPercentile(25);
            double q3 = stats.getPercentile(75);
            double iqr = q3 - q1;
            double lowerBound = q1 - (1.5 * iqr);
            double upperBound = q3 + (1.5 * iqr);
            

            NormalDistribution normalDist = new NormalDistribution(mean, std);
            List<Map<String, Object>> outliers = new ArrayList<>();
            
            for (int i = 0; i < values.length; i++) {
                double zscore = (values[i] - mean) / std;
                if (Math.abs(zscore) > 3 || values[i] < lowerBound || values[i] > upperBound) {
                    Map<String, Object> outlier = new HashMap<>();
                    outlier.put("value", values[i]);
                    outlier.put("zscore", zscore);
                    outlier.put("index", i);
                    outlier.put("probability", normalDist.cumulativeProbability(values[i]));
                    outliers.add(outlier);
                }
            }
            
            Map<String, Object> columnAnalysis = new HashMap<>();
            columnAnalysis.put("column", column);
            columnAnalysis.put("outlier_count", outliers.size());
            columnAnalysis.put("outlier_percentage", 
                (double) outliers.size() / values.length * 100);
            columnAnalysis.put("lower_bound", lowerBound);
            columnAnalysis.put("upper_bound", upperBound);
            columnAnalysis.put("outliers", outliers);
            
            analysis.add(columnAnalysis);
        }
        
        return new DataFrame(analysis);
    }

    /**
     * Adds basic numeric analysis statistics to the analysis map for a given column.
     * 
     * @param df The DataFrame containing the data
     * @param column The column name to analyze
     * @param analysis The map to store the analysis results
     */
    private void addNumericAnalysis(DataFrame df, String column, 
                                  Map<String, Object> analysis) {
        double[] values = getColumnValues(df, column);
        DescriptiveStatistics stats = new DescriptiveStatistics(values);
        
        analysis.put("mean", stats.getMean());
        analysis.put("std", stats.getStandardDeviation());
        analysis.put("min", stats.getMin());
        analysis.put("max", stats.getMax());
        analysis.put("median", stats.getPercentile(50));
    }

    /**
     * Checks if a column contains numeric values.
     * 
     * @param df The DataFrame to check
     * @param column The column name to verify
     * @return true if the column contains numeric values, false otherwise
     */
    private boolean isNumericColumn(DataFrame df, String column) {
        return df.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .anyMatch(val -> val instanceof Number);
    }

    /**
     * Extracts numeric values from a DataFrame column.
     * 
     * @param df The DataFrame containing the data
     * @param column The column name to extract values from
     * @return Array of double values from the column
     */
    private double[] getColumnValues(DataFrame df, String column) {
        return df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .mapToDouble(val -> ((Number) val).doubleValue())
            .toArray();
    }
    
    /**
     * Analyzes missing values in all columns of the DataFrame.
     * 
     * @param df The DataFrame to analyze
     * @return DataFrame containing missing value analysis results including counts
     *         and percentages for each column
     */
    public DataFrame missingAnalysis(DataFrame df) {
        List<Map<String, Object>> analysis = new ArrayList<>();
        

        for (String column : df.getData().get(0).keySet()) {
            Map<String, Object> columnAnalysis = new HashMap<>();
            columnAnalysis.put("column", column);
          
            DescriptiveStatistics stats = new DescriptiveStatistics();
            long totalCount = df.getData().size();
            long missingCount = df.getData().stream()
                .map(row -> row.get(column))
                .filter(Objects::isNull)
                .count();
            
       
            columnAnalysis.put("total_count", totalCount);
            columnAnalysis.put("missing_count", missingCount);
            columnAnalysis.put("missing_percentage", (double) missingCount / totalCount * 100);
            columnAnalysis.put("complete_count", totalCount - missingCount);
            columnAnalysis.put("complete_percentage", 
                (double) (totalCount - missingCount) / totalCount * 100);
            
           
            if (isNumericColumn(df, column)) {
                addNumericAnalysis(df, column, columnAnalysis);
            }
            
            analysis.add(columnAnalysis);
        }
        
        return new DataFrame(analysis);
    }

    /**
     * Generates a comprehensive statistical report for specified columns.
     * 
     * @param df The DataFrame to analyze
     * @param columns The columns to include in the report (if empty, all columns are analyzed)
     * @return Map containing detailed statistical analysis including basic statistics,
     *         missing values, outliers, correlations, and distribution analysis
     */
      public Map<String, Object> fullReport(DataFrame df, String... columns) {
          Map<String, Object> report = new HashMap<>();


          String[] columnsToAnalyze = columns.length > 0 ? 
              columns : 
              df.getData().get(0).keySet().toArray(new String[0]);

  
          report.put("basic_statistics", calculateBasicStats(df, columnsToAnalyze));

        
          report.put("missing_analysis", analyzeMissingValues(df, columnsToAnalyze));

          report.put("outlier_analysis", analyzeOutliers(df, columnsToAnalyze));

      
          report.put("correlation_matrix", calculateCorrelations(df, columnsToAnalyze));

       
          report.put("distribution_analysis", analyzeDistributions(df, columnsToAnalyze));

        
          report.put("summary", createSummary(df, columnsToAnalyze));

          return report;
      }
      
    /**
     * Calculates basic statistical measures for numeric columns.
     * 
     * @param df The DataFrame containing the data
     * @param columns The columns to analyze
     * @return Map of column names to their basic statistical measures
     */
      private Map<String, Map<String, Double>> calculateBasicStats(DataFrame df, String[] columns) {
          Map<String, Map<String, Double>> stats = new HashMap<>();

          for (String column : columns) {
              if (isNumericColumn(df, column)) {
                  DescriptiveStatistics descriptiveStats = new DescriptiveStatistics(getColumnValues(df, column));
                  Map<String, Double> columnStats = new HashMap<>();
                  columnStats.put("mean", descriptiveStats.getMean());
                  columnStats.put("median", descriptiveStats.getPercentile(50));
                  columnStats.put("std", descriptiveStats.getStandardDeviation());
                  columnStats.put("min", descriptiveStats.getMin());
                  columnStats.put("max", descriptiveStats.getMax());
                  columnStats.put("skewness", descriptiveStats.getSkewness());
                  columnStats.put("kurtosis", descriptiveStats.getKurtosis());
                  stats.put(column, columnStats);
              }
          }

          return stats;
      }

       /**
     * Analyzes missing values in specified columns.
     * 
     * @param df The DataFrame to analyze
     * @param columns The columns to check for missing values
     * @return Map containing missing value statistics for each column
     */
      private Map<String, Map<String, Object>> analyzeMissingValues(DataFrame df, String[] columns) {
          Map<String, Map<String, Object>> missingStats = new HashMap<>();

          for (String column : columns) {
              long totalCount = df.getData().size();
              long missingCount = df.getData().stream()
                  .map(row -> row.get(column))
                  .filter(Objects::isNull)
                  .count();

              Map<String, Object> columnStats = new HashMap<>();
              columnStats.put("missing_count", missingCount);
              columnStats.put("missing_percentage", (double) missingCount / totalCount * 100);
              columnStats.put("complete_count", totalCount - missingCount);

              missingStats.put(column, columnStats);
          }

          return missingStats;
      }

      /**
     * Analyzes outliers in specified columns using the IQR method.
     * 
     * @param df The DataFrame to analyze
     * @param columns The columns to check for outliers
     * @return Map containing outlier statistics for each numeric column
     */
      private Map<String, Map<String, Object>> analyzeOutliers(DataFrame df, String[] columns) {
          Map<String, Map<String, Object>> outlierStats = new HashMap<>();

          for (String column : columns) {
              if (isNumericColumn(df, column)) {
                  double[] values = getColumnValues(df, column);
                  DescriptiveStatistics stats = new DescriptiveStatistics(values);

                  double q1 = stats.getPercentile(25);
                  double q3 = stats.getPercentile(75);
                  double iqr = q3 - q1;
                  double lowerBound = q1 - (1.5 * iqr);
                  double upperBound = q3 + (1.5 * iqr);

                  long outlierCount = Arrays.stream(values)
                      .filter(v -> v < lowerBound || v > upperBound)
                      .count();

                  Map<String, Object> columnStats = new HashMap<>();
                  columnStats.put("outlier_count", outlierCount);
                  columnStats.put("outlier_percentage", (double) outlierCount / values.length * 100);
                  columnStats.put("lower_bound", lowerBound);
                  columnStats.put("upper_bound", upperBound);

                  outlierStats.put(column, columnStats);
              }
          }

          return outlierStats;
      }

      /**
     * Calculates correlation matrix for numeric columns.
     * 
     * @param df The DataFrame containing the data
     * @param columns The columns to include in correlation analysis
     * @return Map containing correlation coefficients between all pairs of numeric columns
     */
      private Map<String, Map<String, Double>> calculateCorrelations(DataFrame df, String[] columns) {
          Map<String, Map<String, Double>> correlations = new HashMap<>();

          List<String> numericColumns = Arrays.stream(columns)
              .filter(col -> isNumericColumn(df, col))
              .collect(Collectors.toList());

          for (String col1 : numericColumns) {
              Map<String, Double> columnCorr = new HashMap<>();
              double[] values1 = getColumnValues(df, col1);

              for (String col2 : numericColumns) {
                  double[] values2 = getColumnValues(df, col2);
                  double correlation = new PearsonsCorrelation().correlation(values1, values2);
                  columnCorr.put(col2, correlation);
              }

              correlations.put(col1, columnCorr);
          }

          return correlations;
      }

      /**
     * Analyzes the distribution characteristics of numeric columns.
     * 
     * @param df The DataFrame containing the data
     * @param columns The columns to analyze
     * @return Map containing distribution analysis for each numeric column
     */
      private Map<String, Map<String, Object>> analyzeDistributions(DataFrame df, String[] columns) {
          Map<String, Map<String, Object>> distributions = new HashMap<>();

          for (String column : columns) {
              if (isNumericColumn(df, column)) {
                  double[] values = getColumnValues(df, column);
                  DescriptiveStatistics stats = new DescriptiveStatistics(values);

                  Map<String, Object> distStats = new HashMap<>();
                  distStats.put("normality_test", performNormalityTest(values));
                  distStats.put("percentiles", calculatePercentiles(stats));
                  distStats.put("distribution_type", determineDistributionType(stats));

                  distributions.put(column, distStats);
              }
          }

          return distributions;
      }
      
     /**
     * Performs normality test on a set of values.
     * 
     * @param values Array of values to test for normality
     * @return Map containing normality test statistics including skewness and kurtosis
     */
      private Map<String, Double> performNormalityTest(double[] values) {
          Map<String, Double> normalityStats = new HashMap<>();
          DescriptiveStatistics stats = new DescriptiveStatistics(values);
          double skewness = stats.getSkewness();
          double kurtosis = stats.getKurtosis();

          normalityStats.put("skewness", skewness);
          normalityStats.put("kurtosis", kurtosis);
          normalityStats.put("normal_probability", 
              Math.exp(-(Math.pow(skewness, 2) + Math.pow(kurtosis - 3, 2))/6));

          return normalityStats;
      }

      /**
     * Calculates percentiles at 25% intervals for a set of statistics.
     * 
     * @param stats DescriptiveStatistics object containing the data
     * @return Map of percentile labels to their values
     */
      private Map<String, Double> calculatePercentiles(DescriptiveStatistics stats) {
          Map<String, Double> percentiles = new HashMap<>();
          for (int i = 0; i <= 100; i += 25) {
              percentiles.put("p" + i, stats.getPercentile(i));
          }
          return percentiles;
      }

      /**
     * Determines the type of distribution based on skewness and kurtosis.
     * 
     * @param stats DescriptiveStatistics object containing the data
     * @return String describing the distribution type (Normal, Right-skewed, Left-skewed, or Non-normal)
     */
      private String determineDistributionType(DescriptiveStatistics stats) {
          double skewness = stats.getSkewness();
          double kurtosis = stats.getKurtosis();

          if (Math.abs(skewness) < 0.5 && Math.abs(kurtosis - 3) < 0.5) {
              return "Normal";
          } else if (skewness > 1) {
              return "Right-skewed";
          } else if (skewness < -1) {
              return "Left-skewed";
          } else {
              return "Non-normal";
          }
      }

      
    /**
     * Creates a summary of the DataFrame and analysis results.
     * 
     * @param df The DataFrame being analyzed
     * @param columns The columns included in the analysis
     * @return Map containing summary statistics and metadata about the analysis
     */
      private Map<String, Object> createSummary(DataFrame df, String[] columns) {
          Map<String, Object> summary = new HashMap<>();
          summary.put("total_rows", df.getData().size());
          summary.put("total_columns", columns.length);
          summary.put("numeric_columns", 
              Arrays.stream(columns).filter(col -> isNumericColumn(df, col)).count());
          summary.put("categorical_columns",
              Arrays.stream(columns).filter(col -> !isNumericColumn(df, col)).count());
          summary.put("timestamp", System.currentTimeMillis());
          return summary;
      }
}