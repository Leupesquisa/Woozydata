package com.leumanuel.woozydata;

import com.leumanuel.woozydata.config.MongoConfig;
import com.leumanuel.woozydata.controller.DataController;
import com.leumanuel.woozydata.model.DataFrame;
import com.leumanuel.woozydata.repository.MongoDbConnector;
import com.leumanuel.woozydata.service.AdvancedAnalysisService;
import com.leumanuel.woozydata.service.DataAnalysisService;
import com.leumanuel.woozydata.service.DataCleaningService;
import com.leumanuel.woozydata.service.DataExportService;
import com.leumanuel.woozydata.service.DataStatisticsService;
import com.leumanuel.woozydata.service.MergeService;
import com.leumanuel.woozydata.service.ProbabilityService;
import com.leumanuel.woozydata.service.RegressionService;
import com.leumanuel.woozydata.service.StatisticalService;
import com.leumanuel.woozydata.service.TimeSeriesAnalysisService;
import com.leumanuel.woozydata.util.DataCsvReader;
import com.leumanuel.woozydata.util.DataJsonReader;
import com.leumanuel.woozydata.util.DataXlsxReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *  *WoozyData Library*
 * A comprehensive Java library for data analysis, providing a unified interface for 
 * data manipulation, statistical analysis, and machine learning operations.
 * 
 * <p>Key features include:
 * <ul>
 *   <li>Data loading from multiple sources (CSV, Excel, JSON, MongoDB)</li>
 *   <li>Statistical analysis and data cleaning</li>
 *   <li>Time series analysis</li>
 *   <li>Data visualization and export capabilities</li>
 *   <li>Machine learning operations</li>
 * </ul>
 * 
 * @author Leu A. Manuel, github.com/Leupesquisa
 * @version 1.0
 */
public class Woozydata implements DataController {
    
    private final DataAnalysisService analysisService;
    private final DataCleaningService cleaningService;
    private final DataExportService exportService;
    private final DataStatisticsService statisticsService;
    private final ProbabilityService probabilityService;
    private final RegressionService regressionService;
    private final StatisticalService statisticalService;
    private final TimeSeriesAnalysisService timeSeriesService;
    private final MergeService mergeService;
    private final AdvancedAnalysisService advancedAnalysisService;
    
    private DataFrame currentDataFrame;

    /**
     * Initializes a new instance of Woozydata with all required analysis services.
     * This constructor sets up all necessary services for data analysis operations.
     */
    public Woozydata() {
        this.analysisService = new DataAnalysisService();
        this.cleaningService = new DataCleaningService();
        this.exportService = new DataExportService();
        this.statisticsService = new DataStatisticsService();
        this.probabilityService = new ProbabilityService();
        this.regressionService = new RegressionService();
        this.statisticalService = new StatisticalService();
        this.timeSeriesService = new TimeSeriesAnalysisService();
        this.mergeService = new MergeService();
        this.advancedAnalysisService = new AdvancedAnalysisService();
    }

    // =============== Data Loading Operations ===============
    
    /**
     * Loads data from a CSV file into a DataFrame.
     * 
     * @param filePath Path to the CSV file
     * @return DataFrame containing the loaded data
     * @throws Exception if file cannot be read or is invalid
     */
    @Override
    public DataFrame fromCsv(String filePath) throws Exception {
        currentDataFrame = DataCsvReader.readCSV(filePath);
        return currentDataFrame;
    }

    /**
     * Loads data from an Excel file into a DataFrame.
     * 
     * @param filePath Path to the Excel file (.xlsx)
     * @return DataFrame containing the loaded data
     * @throws Exception if file cannot be read or is invalid
     */
    @Override
    public DataFrame fromXlsx(String filePath) throws Exception {
        currentDataFrame = DataXlsxReader.readXLSX(filePath);
        return currentDataFrame;
    }
    
    /**
     * Loads data from a JSON file into a DataFrame.
     * 
     * @param filePath Path to the JSON file
     * @return DataFrame containing the loaded data
     * @throws Exception if file cannot be read or is invalid
     */
    @Override
    public DataFrame fromJson(String filePath) throws Exception {
        currentDataFrame = DataJsonReader.readJSON(filePath);
        return currentDataFrame;
    }

     /**
     * Connects to MongoDB and loads data from a collection into a DataFrame.
     * 
     * @param connectionString MongoDB connection string
     * @param dbName Database name
     * @param collection Collection name
     * @return DataFrame containing the loaded data
     */
    @Override
    public DataFrame fromMongo(String connectionString, String dbName, String collection) {
        MongoConfig config = new MongoConfig(connectionString, dbName);
        MongoDbConnector connector = new MongoDbConnector(config);
        currentDataFrame = connector.readCollection(collection);
        connector.close();
        return currentDataFrame;
    }

// =============== Basic Statistical Methods ===============
    
    /**
     * Calculates the arithmetic mean of a numeric column.
     * 
     * @param column Name of the column
     * @return Mean value of the column
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalArgumentException if column is not numeric
     */    
    @Override
    public double mean(String column) {
        validateDataFrame();
        return analysisService.calculateMean(currentDataFrame, column);
    }

    /**
     * Calculates the median value of a numeric column.
     * 
     * @param column Name of the column
     * @return Median value of the column
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalArgumentException if column is not numeric
     */
    @Override
    public double median(String column) {
        validateDataFrame();
        return analysisService.calculateMedian(currentDataFrame, column);
    }
    
/**
     * Calculates the standard deviation of a numeric column.
     * 
     * @param column Name of the column
     * @return Standard deviation of the column
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalArgumentException if column is not numeric
     */
    @Override
    public double stdv(String column) {
        validateDataFrame();
        return analysisService.calculateStandardDeviation(currentDataFrame, column);
    }
    
    
// =============== Data Cleaning Methods ===============
    
    /**
     * Calculates the variance of a numeric column.
     * 
     * @param column Name of the column
     * @return Variance value of the column
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalArgumentException if column is not numeric
     */
    @Override
    public double vars(String column) {
        validateDataFrame();
        return analysisService.calculateVariance(currentDataFrame, column);
    }

    /**
     * Calculates the skewness of a numeric column.
     * 
     * @param column Name of the column
     * @return Skewness value of the column
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalArgumentException if column is not numeric
     */
    @Override
    public double skew(String column) {
        validateDataFrame();
        return statisticsService.calculateSkewness(currentDataFrame, column);
    }
    
     /**
     * Calculates the kurtosis of a numeric column.
     * 
     * @param column Name of the column
     * @return Kurtosis value of the column
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalArgumentException if column is not numeric
     */
    @Override
    public double kurt(String column) {
        validateDataFrame();
        return statisticsService.calculateKurtosis(currentDataFrame, column);
    }

    @Override
    public double cov(String col1, String col2) {
        validateDataFrame();
        return statisticsService.calculateCovariance(currentDataFrame, col1, col2);
    }

    // =============== Data Cleaning Methods ===============
    
    /**
     * Performs automatic data cleaning on the current DataFrame.
     * This method combines multiple cleaning operations:
     * <ul>
     *   <li>Removes missing values</li>
     *   <li>Removes duplicates</li>
     *   <li>Fixes data types</li>
     *   <li>Standardizes formats</li>
     * </ul>
     * 
     * @return Cleaned DataFrame
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame clean() {
        validateDataFrame();
        currentDataFrame = cleaningService.clean(currentDataFrame);
        return currentDataFrame;
    }

     /**
     * Removes rows containing null values from the DataFrame.
     * 
     * @return DataFrame with null values removed
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame dropNa() {
        validateDataFrame();
        currentDataFrame = cleaningService.dropNa(currentDataFrame);
        return currentDataFrame;
    }
    
    
    /**
     * Removes duplicate rows based on specified columns.
     * 
     * @param columns Column names to check for duplicates. If none specified,
     *               checks all columns
     * @return DataFrame with duplicates removed
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame dropDupes(String... columns) {
        validateDataFrame();
        currentDataFrame = cleaningService.dropDuplicates(currentDataFrame, columns);
        return currentDataFrame;
    }

    
    /**
     * Fills null values in all columns with a specified value.
     * 
     * @param value Value to use for filling null values
     * @return DataFrame with filled values
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame fillNa(Object value) {
        validateDataFrame();
        currentDataFrame = cleaningService.fillNa(currentDataFrame, value);
        return currentDataFrame;
    }

    
    
    @Override
    public DataFrame fillNaColumns(Object value, String... columns) {
        validateDataFrame();
        currentDataFrame = cleaningService.fillNaColumns(currentDataFrame, value, columns);
        return currentDataFrame;
    }
    
    /**
     * Validates if a DataFrame is currently loaded.
     * 
     * @throws IllegalStateException if no DataFrame is loaded
     */    
    private void validateDataFrame() {
        if (currentDataFrame == null) {
            throw new IllegalStateException("No DataFrame loaded. Load data first using from* methods.");
        }
    }

    // Helper method to get numeric values from a column
    private double[] getNumericValues(String column) {
        return currentDataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .mapToDouble(val -> ((Number) val).doubleValue())
            .toArray();
    }

   // =============== Export Methods ===============
    
    /**
     * Exports DataFrame to CSV file.
     * 
     * @param filePath Output file path
     * @throws Exception if export fails
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public void toCsv(String filePath) throws Exception {
        validateDataFrame();
        exportService.exportToCSV(currentDataFrame, filePath);
    }
    
     /**
     * Exports DataFrame to JSON format.
     * 
     * @param filePath Path where to save the JSON file
     * @throws Exception if export fails
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public void toJson(String filePath) throws Exception {
        validateDataFrame();
        exportService.exportToJSON(currentDataFrame, filePath);
    }

    /**
     * Exports DataFrame to Excel format.
     * 
     * @param filePath Path where to save the Excel file
     * @throws Exception if export fails
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public void toExcel(String filePath) throws Exception {
        validateDataFrame();
        exportService.exportToExcel(currentDataFrame, filePath);
    }

    /**
     * Exports DataFrame to PowerBI format.
     * Includes:
     * <ul>
     *   <li>Data sheet</li>
     *   <li>Metadata sheet</li>
     *   <li>Statistics sheet</li>
     * </ul>
     * 
     * @param filePath Output file path
     * @throws Exception if export fails
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public void toPowerBI(String filePath) throws Exception {
        validateDataFrame();
        Map<String, DataFrame> exports = new HashMap<>();
        exports.put("data", currentDataFrame);
        exports.put("metadata", createMetadata());
        exports.put("statistics", calculateStatistics());
        exportService.exportToPowerBI(exports, filePath);
    }

    @Override
    public void toHtml(String filePath) throws Exception {
        validateDataFrame();
        exportService.exportToHTML(currentDataFrame, filePath);
    }

    /**
     * Exports DataFrame to LaTeX format for academic papers.
     * 
     * @param filePath Output file path
     * @throws Exception if export fails
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public void toLatex(String filePath) throws Exception {
        validateDataFrame();
        exportService.exportToLatex(currentDataFrame, filePath);
    }

    // =============== Advanced Statistical Methods ===============
    
    /**
     * Generates comprehensive statistical analysis of a column.
     * Includes:
     * <ul>
     *   <li>Basic statistics (mean, median, std)</li>
     *   <li>Distribution analysis</li>
     *   <li>Missing value analysis</li>
     *   <li>Outlier detection</li>
     * </ul>
     * 
     * @param column Column name to analyze
     * @return Map containing statistical measures
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public Map<String, Object> describe(String column) {
        validateDataFrame();
        Map<String, Object> description = new HashMap<>();
        
        description.put("count", count(column));
        description.put("mean", mean(column));
        description.put("std", stdv(column));
        description.put("min", min(column));
        description.put("25%", quantile(column, 0.25));
        description.put("50%", median(column));
        description.put("75%", quantile(column, 0.75));
        description.put("max", max(column));
        description.put("skewness", skew(column));
        description.put("kurtosis", kurt(column));
        description.put("missing", countMissing(column));
        
        return description;
    }

    
    /**
     * Calculates quantile value for a numeric column.
     * 
     * @param column Column name
     * @param q Quantile value (0-1)
     * @return Quantile value
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalArgumentException if q is not between 0 and 1
     */
    @Override
    public double quantile(String column, double q) {
        validateDataFrame();
        return statisticsService.calculateQuantile(currentDataFrame, column, q);
    }

    
     /**
     * Calculates the Interquartile Range (IQR) of a column.
     * IQR is the difference between the 75th and 25th percentiles.
     * 
     * @param column Column name
     * @return IQR value
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public double iqr(String column) {
        validateDataFrame();
        return statisticsService.calculateIQR(currentDataFrame, column);
    }

    
    
    @Override
    public Map<Object, Long> frequency(String column) {
        validateDataFrame();
        return statisticsService.calculateFrequency(currentDataFrame, column);
    }

    // Probability Methods
    @Override
    public double[] normalDist(int size, double mean, double std) {
        return probabilityService.generateNormalDistribution(size, mean, std);
    }

    @Override
    public double normalPdf(double x, double mean, double std) {
        return probabilityService.normalPDF(x, mean, std);
    }

    @Override
    public double normalCdf(double x, double mean, double std) {
        return probabilityService.normalCDF(x, mean, std);
    }

    @Override
    public double[] poissonDist(double lambda, int size) {
        return probabilityService.generatePoissonDistribution(lambda, size);
    }

    @Override
    public double[] uniformDist(int size, double min, double max) {
        return probabilityService.generateUniformDistribution(size, min, max);
    }

    // Regression and Correlation Methods
    @Override
    public double correl(String col1, String col2) {
        validateDataFrame();
        return regressionService.calculatePearsonCorrelation(currentDataFrame, col1, col2);
    }

    @Override
    public double[] linearReg(String xCol, String yCol) {
        validateDataFrame();
        return regressionService.simpleLinearRegression(currentDataFrame, xCol, yCol);
    }

    @Override
    public double rsquared(String xCol, String yCol) {
        validateDataFrame();
        return regressionService.calculateRSquared(currentDataFrame, xCol, yCol);
    }

    @Override
    public DataFrame multipleReg(String[] xCols, String yCol) {
        validateDataFrame();
        return regressionService.multipleRegression(currentDataFrame, xCols, yCol);
    }

    @Override
    public DataFrame polynomialReg(String xCol, String yCol, int degree) {
        validateDataFrame();
        return regressionService.polynomialRegression(currentDataFrame, xCol, yCol, degree);
    }

    @Override
    public DataFrame logisticReg(String xCol, String yCol) {
        validateDataFrame();
        return regressionService.logisticRegression(currentDataFrame, xCol, yCol);
    }

    // Statistical Tests
    @Override
    public Map<String, Double> tTest(String col1, String col2) {
        validateDataFrame();
        return statisticalService.tTest(currentDataFrame, col1, col2);
    }

    @Override
    public Map<String, Double> anova(String... columns) {
        validateDataFrame();
        return statisticalService.anova(currentDataFrame, columns);
    }

    @Override
    public Map<String, Double> chiSquare(String col1, String col2) {
        validateDataFrame();
        return statisticalService.chiSquareTest(currentDataFrame, col1, col2);
    }

    @Override
    public Map<String, Double> shapiroWilk(String column) {
        validateDataFrame();
        return statisticalService.shapiroWilkTest(currentDataFrame, column);
    }

    private DataFrame createMetadata() {
        List<Map<String, Object>> metadata = new ArrayList<>();
        currentDataFrame.getData().get(0).forEach((column, value) -> {
            Map<String, Object> columnMetadata = new HashMap<>();
            columnMetadata.put("column", column);
            columnMetadata.put("type", value != null ? value.getClass().getSimpleName() : "null");
            columnMetadata.put("missing", countMissing(column));
            columnMetadata.put("unique", countUnique(column));
            metadata.add(columnMetadata);
        });
        return new DataFrame(metadata);
    }

    private DataFrame calculateStatistics() {
        List<Map<String, Object>> stats = new ArrayList<>();
        currentDataFrame.getData().get(0).keySet().forEach(column -> {
            if (isNumeric(column)) {
                Map<String, Object> columnStats = new HashMap<>();
                columnStats.putAll(describe(column));
                stats.add(columnStats);
            }
        });
        return new DataFrame(stats);
    }

    private boolean isNumeric(String column) {
        return currentDataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::nonNull)
            .anyMatch(value -> value instanceof Number);
    }

    private long countMissing(String column) {
        return currentDataFrame.getData().stream()
            .map(row -> row.get(column))
            .filter(Objects::isNull)
            .count();
    }

    private long countUnique(String column) {
        return currentDataFrame.getData().stream()
            .map(row -> row.get(column))
            .distinct()
            .count();
    }
    
    // =============== Time Series Methods ===============
    
    /**
     * Calculates Simple Moving Average (SMA) for time series data.
     * 
     * @param data Input time series data
     * @param window Window size for moving average
     * @return Array containing SMA values
     */
    @Override
    public double[] sma(double[] data, int window) {
        validateDataFrame();
        return timeSeriesService.simpleMovingAverage(data, window);
    }

    @Override
    public double[] ema(double[] data, double alpha) {
        validateDataFrame();
        return timeSeriesService.exponentialMovingAverage(data, alpha);
    }

    @Override
    public DataFrame forecast(String timeCol, String valueCol, int periods) {
        validateDataFrame();
        return timeSeriesService.forecast(currentDataFrame, timeCol, valueCol, periods);
    }

    @Override
    public DataFrame decompose(String timeCol, String valueCol) {
        validateDataFrame();
        return timeSeriesService.decompose(currentDataFrame, timeCol, valueCol);
    }

    @Override
    public DataFrame seasonalAdjust(String timeCol, String valueCol) {
        validateDataFrame();
        return timeSeriesService.seasonalAdjustment(currentDataFrame, timeCol, valueCol);
    }

    @Override
    public DataFrame detectOutliers(String timeCol, String valueCol) {
        validateDataFrame();
        return timeSeriesService.detectTimeSeriesOutliers(currentDataFrame, timeCol, valueCol);
    }

    // =============== Data Transformation Methods ===============
    
    /**
     * Creates a pivot table from the DataFrame.
     * 
     * @param index Column to use as index
     * @param columns Column to use for new columns
     * @param values Column to use for values
     * @return Pivoted DataFrame
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame pivot(String index, String columns, String values) {
        validateDataFrame();
        Map<Object, Map<Object, List<Object>>> pivotData = currentDataFrame.getData().stream()
            .collect(Collectors.groupingBy(
                row -> row.get(index),
                Collectors.groupingBy(
                    row -> row.get(columns),
                    Collectors.mapping(row -> row.get(values), Collectors.toList())
                )
            ));

        return createPivotDataFrame(pivotData);
    }

    /**
     * Reshapes data from wide to long format.
     * 
     * @param idVars Columns to use as identifiers
     * @param valueVars Columns to unpivot
     * @return Melted DataFrame
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame melt(String[] idVars, String[] valueVars) {
        validateDataFrame();
        List<Map<String, Object>> meltedData = new ArrayList<>();
        
        for (Map<String, Object> row : currentDataFrame.getData()) {
            for (String valueVar : valueVars) {
                Map<String, Object> newRow = new HashMap<>();
                // Add ID variables
                for (String idVar : idVars) {
                    newRow.put(idVar, row.get(idVar));
                }
                // Add value variables
                newRow.put("variable", valueVar);
                newRow.put("value", row.get(valueVar));
                meltedData.add(newRow);
            }
        }
        
        return new DataFrame(meltedData);
    }

    @Override
    public DataFrame dummies(String... columns) {
        validateDataFrame();
        List<Map<String, Object>> dummyData = new ArrayList<>();
        
        for (Map<String, Object> row : currentDataFrame.getData()) {
            Map<String, Object> newRow = new HashMap<>(row);
            
            for (String column : columns) {
                Object value = row.get(column);
                if (value != null) {
                    String dummyColumn = column + "_" + value.toString();
                    newRow.put(dummyColumn, 1);
                }
            }
            
            dummyData.add(newRow);
        }
        
        return new DataFrame(dummyData);
    }

    @Override
    public DataFrame bin(String column, int bins) {
        validateDataFrame();
        double[] values = getNumericValues(column);
        double min = Arrays.stream(values).min().orElse(0);
        double max = Arrays.stream(values).max().orElse(0);
        double binWidth = (max - min) / bins;
        
        List<Map<String, Object>> binnedData = new ArrayList<>();
        
        for (Map<String, Object> row : currentDataFrame.getData()) {
            Map<String, Object> newRow = new HashMap<>(row);
            double value = ((Number) row.get(column)).doubleValue();
            int binIndex = (int) ((value - min) / binWidth);
            if (binIndex == bins) binIndex--; // Handle maximum value
            newRow.put(column + "_bin", binIndex);
            binnedData.add(newRow);
        }
        
        return new DataFrame(binnedData);
    }

    @Override
    public DataFrame rollingWindow(String column, int window, String func) {
        validateDataFrame();
        double[] values = getNumericValues(column);
        double[] result = new double[values.length];
        
        for (int i = 0; i < values.length; i++) {
            int start = Math.max(0, i - window + 1);
            double[] windowValues = Arrays.copyOfRange(values, start, i + 1);
            result[i] = calculateWindowStat(windowValues, func);
        }
        
        List<Map<String, Object>> rollingData = new ArrayList<>();
        for (int i = 0; i < currentDataFrame.getData().size(); i++) {
            Map<String, Object> newRow = new HashMap<>(currentDataFrame.getData().get(i));
            newRow.put(column + "_rolling_" + func, result[i]);
            rollingData.add(newRow);
        }
        
        return new DataFrame(rollingData);
    }

    // Quick Analysis Methods
    @Override
    public DataFrame quickAnalysis(String... columns) {
        validateDataFrame();
        List<Map<String, Object>> analysis = new ArrayList<>();
        
        for (String column : columns) {
            Map<String, Object> columnAnalysis = new HashMap<>();
            columnAnalysis.put("column", column);
            columnAnalysis.putAll(describe(column));
            
            // Add additional quick insights
            columnAnalysis.put("missing_percentage", 
                (double) countMissing(column) / currentDataFrame.getData().size() * 100);
            columnAnalysis.put("unique_percentage", 
                (double) countUnique(column) / currentDataFrame.getData().size() * 100);
            
            if (isNumeric(column)) {
                columnAnalysis.put("outliers", detectOutliers(column).size());
                columnAnalysis.put("distribution_type", detectDistributionType(column));
            }
            
            analysis.add(columnAnalysis);
        }
        
        return new DataFrame(analysis);
    }

    @Override
    public DataFrame correlation(String... columns) {
        validateDataFrame();
        String[] cols = columns.length > 0 ? columns : 
            currentDataFrame.getData().get(0).keySet().toArray(new String[0]);
        
        List<Map<String, Object>> correlationMatrix = new ArrayList<>();
        
        for (String col1 : cols) {
            if (!isNumeric(col1)) continue;
            
            Map<String, Object> rowData = new HashMap<>();
            rowData.put("column", col1);
            
            for (String col2 : cols) {
                if (!isNumeric(col2)) continue;
                rowData.put(col2, correl(col1, col2));
            }
            
            correlationMatrix.add(rowData);
        }
        
        return new DataFrame(correlationMatrix);
    }

    // Optimization Methods
    private double calculateWindowStat(double[] values, String func) {
        return switch (func.toLowerCase()) {
            case "mean" -> Arrays.stream(values).average().orElse(Double.NaN);
            case "sum" -> Arrays.stream(values).sum();
            case "min" -> Arrays.stream(values).min().orElse(Double.NaN);
            case "max" -> Arrays.stream(values).max().orElse(Double.NaN);
            default -> Double.NaN;
        };
    }

    private List<Double> detectOutliers(String column) {
        double[] values = getNumericValues(column);
        double q1 = calculateQuantile(values, 0.25);
        double q3 = calculateQuantile(values, 0.75);
        double iqr = q3 - q1;
        double lowerBound = q1 - 1.5 * iqr;
        double upperBound = q3 + 1.5 * iqr;
        
        return Arrays.stream(values)
            .filter(v -> v < lowerBound || v > upperBound)
            .boxed()
            .collect(Collectors.toList());
    }

    private String detectDistributionType(String column) {
        double skewness = skew(column);
        double kurtosis = kurt(column);
        
        if (Math.abs(skewness) < 0.5 && Math.abs(kurtosis - 3) < 0.5) {
            return "Normal";
        } else if (skewness > 1) {
            return "Right Skewed";
        } else if (skewness < -1) {
            return "Left Skewed";
        } else {
            return "Unknown";
        }
    }

    private double calculateQuantile(double[] values, double quantile) {
        Arrays.sort(values);
        int index = (int) (quantile * values.length);
        return values[index];
    }

    private DataFrame createPivotDataFrame(Map<Object, Map<Object, List<Object>>> pivotData) {
        List<Map<String, Object>> result = new ArrayList<>();
        Set<Object> columns = new HashSet<>();
        
        // Get all unique column values
        pivotData.values().forEach(map -> columns.addAll(map.keySet()));
        
        // Create rows
        for (Map.Entry<Object, Map<Object, List<Object>>> entry : pivotData.entrySet()) {
            Map<String, Object> row = new HashMap<>();
            row.put("index", entry.getKey());
            
            for (Object column : columns) {
                List<Object> values = entry.getValue().getOrDefault(column, Collections.emptyList());
                row.put(column.toString(), calculateAverage(values));
            }
            
            result.add(row);
        }
        
        return new DataFrame(result);
    }

    private double calculateAverage(List<Object> values) {
        if (values.isEmpty()) return 0.0;
        return values.stream()
            .filter(v -> v instanceof Number)
            .mapToDouble(v -> ((Number) v).doubleValue())
            .average()
            .orElse(0.0);
    }

/**
     * Calculates basic count of non-null values in a column.
     * 
     * @param column Name of the column to count
     * @return Count of non-null values
     * @throws IllegalStateException if no DataFrame is loaded
     */    
 @Override
 public long count(String column) {
     validateDataFrame();
     return currentDataFrame.getData().stream()
         .map(row -> row.get(column))
         .filter(Objects::nonNull)
         .count();
 }

 @Override
 public double min(String column) {
     validateDataFrame();
     return currentDataFrame.getData().stream()
         .map(row -> row.get(column))
         .filter(val -> val instanceof Number)
         .mapToDouble(val -> ((Number) val).doubleValue())
         .min()
         .orElseThrow(() -> new IllegalStateException("No numeric values found in column: " + column));
 }

 
 /**
     * Finds the maximum value in a numeric column.
     * 
     * @param column Name of the column
     * @return Maximum value
     * @throws IllegalStateException if no DataFrame is loaded
     * @throws IllegalStateException if no numeric values found in column
     */
 @Override
 public double max(String column) {
     validateDataFrame();
     return currentDataFrame.getData().stream()
         .map(row -> row.get(column))
         .filter(val -> val instanceof Number)
         .mapToDouble(val -> ((Number) val).doubleValue())
         .max()
         .orElseThrow(() -> new IllegalStateException("No numeric values found in column: " + column));
 }

    // Método auxiliar para verificar se uma coluna é numérica
    private boolean hasNumericValues(String column) {
        return currentDataFrame.getData().stream()
            .map(row -> row.get(column))
            .anyMatch(val -> val instanceof Number);
    }

    /**
     * Analyzes a specific column for statistical measures and patterns.
     * 
     * @param column Name of the column to analyze
     * @return DataFrame containing analysis results
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame analyze(String column) {
        validateDataFrame();
        return analysisService.analyze(currentDataFrame, column);
    }

  
    /**
     * Converts column data types according to the specified type map.
     * 
     * @param typeMap Map of column names to their target Java types
     * @return DataFrame with converted column types
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame convert(Map<String, Class<?>> typeMap) {
        validateDataFrame();
        return cleaningService.convert(currentDataFrame, typeMap);
    }
    
    
    /**
     * Standardizes specified columns using z-score normalization.
     * 
     * @param columns Columns to standardize
     * @return DataFrame with standardized columns
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame standardize(String... columns) {
         validateDataFrame();
        return cleaningService.standardize(currentDataFrame, columns);        
    }
    
    
    /**
     * Normalizes specified columns to range [0,1].
     * 
     * @param columns Columns to normalize
     * @return DataFrame with normalized columns
     * @throws IllegalStateException if no DataFrame is loaded
     */
    @Override
    public DataFrame normalize(String... columns) {
        validateDataFrame();
        return cleaningService.normalize(currentDataFrame, columns);
    }

    @Override
    public double sum(String column) {
        validateDataFrame();
        return analysisService.sum(currentDataFrame, column);
    }

    @Override
    public double avg(String column) {
        validateDataFrame();
        return analysisService.avg(currentDataFrame, column);
    }


    @Override
    public int[] binomialDist(int trials, double prob, int size) {
        return statisticsService.binomialDist(trials, prob, size);
    }

    @Override
    public Map<String, Double> mannWhitney(String col1, String col2) {
        validateDataFrame();
        return statisticsService.mannWhitney(currentDataFrame, col1, col2);
    }

    @Override
    public DataFrame groupBy(String... columns) {
        validateDataFrame();
        return statisticsService.groupBy(currentDataFrame, columns);
    }

    @Override
    public DataFrame sort(String... columns) {
        validateDataFrame();
        return statisticsService.sort(currentDataFrame, columns);
    }

    @Override
    public DataFrame select(String... columns) {
        validateDataFrame();
        return statisticalService.select(currentDataFrame, columns);
    }

    @Override
    public DataFrame sample(int n) {
        validateDataFrame();
        return statisticalService.sample(currentDataFrame, n);
    }

    @Override
    public DataFrame merge(DataFrame other, String how, String... on) {
        validateDataFrame();
        return mergeService.merge(currentDataFrame, other, how, on);
    }

    @Override
    public DataFrame concat(DataFrame other, boolean axis) {
        validateDataFrame();
        return mergeService.concat(currentDataFrame, other, axis);
    }

    @Override
    public DataFrame reshape(int rows, int cols) {
        validateDataFrame();
        return mergeService.reshape(currentDataFrame, rows, cols);
    }

  
   
    @Override
    public DataFrame timeAnalysis(String dateCol, String valueCol) {
        validateDataFrame();
        return advancedAnalysisService.timeAnalysis(currentDataFrame, dateCol, valueCol);
    }

    @Override
    public DataFrame missingAnalysis() {
        validateDataFrame();
        return advancedAnalysisService.missingAnalysis(currentDataFrame);
    }

        // =============== Quick Analysis Methods ===============

   /**
   * Performs quick comprehensive analysis of specified columns.
   * Includes:
   * <ul>
   *   <li>Basic statistics</li>
   *   <li>Missing value analysis</li>
   *   <li>Distribution analysis</li>
   *   <li>Outlier detection</li>
   * </ul>
   * 
   * <p>Example:
   * <pre>
   * DataFrame analysis = woozy.quickAnalysis("sales", "profit");
   * analysis.show();
   * </pre>
   *
   * @param columns Columns to analyze
   * @return DataFrame containing analysis results
   * @throws IllegalStateException if no DataFrame is loaded
   */
    @Override
    public DataFrame outlierAnalysis(String... columns) {
        validateDataFrame();
        return advancedAnalysisService.outlierAnalysis(currentDataFrame, columns);
    }

  
    @Override
    public Map<String, Double> stats(String column) {
         validateDataFrame();
        return cleaningService.stats(currentDataFrame, column);
    }

    @Override
    public DataFrame interpolate(String method, String... columns) {
        validateDataFrame();
        return cleaningService.interpolate(currentDataFrame, method, columns);
    }
    
        /**
   * Generates a full statistical report.
   * 
   * <p>Example:
   * <pre>
   * Map&lt;String, Object&gt; report = woozy.fullReport("sales", "profit");
   * System.out.println("Correlation: " + report.get("correlation"));
   * </pre>
   *
   * @param columns Columns to include in report
   * @return Map containing comprehensive analysis
   * @throws IllegalStateException if no DataFrame is loaded
   */
    @Override
    public Map<String, Object> fullReport(String... columns) {
        validateDataFrame();
        return advancedAnalysisService.fullReport(currentDataFrame, columns);
    }

}