package com.leumanuel.woozydata.controller;

import com.leumanuel.woozydata.model.DataFrame;
import java.util.Map;

/**
 * Interface that defines operations for data manipulation, analysis, and statistical computations
 * similar to popular data analysis libraries like pandas.
 */
public interface DataController {
   
    /**
     * Reads data from a CSV file and creates a DataFrame.
     * @param filePath Path to the CSV file
     * @return DataFrame containing the data from the CSV file
     * @throws Exception If there's an error reading the file
     */
    DataFrame fromCsv(String filePath) throws Exception;

    /**
     * Reads data from an Excel (XLSX) file and creates a DataFrame.
     * @param filePath Path to the Excel file
     * @return DataFrame containing the data from the Excel file
     * @throws Exception If there's an error reading the file
     */
    DataFrame fromXlsx(String filePath) throws Exception;

    /**
     * Reads data from a JSON file and creates a DataFrame.
     * @param filePath Path to the JSON file
     * @return DataFrame containing the data from the JSON file
     * @throws Exception If there's an error reading the file
     */
    DataFrame fromJson(String filePath) throws Exception;

    /**
     * Reads data from a MongoDB collection and creates a DataFrame.
     * @param connectionString MongoDB connection string
     * @param dbName Database name
     * @param collection Collection name
     * @return DataFrame containing the data from MongoDB
     */
    DataFrame fromMongo(String connectionString, String dbName, String collection);

    /**
     * Performs basic analysis on a specified column.
     * @param column Name of the column to analyze
     * @return DataFrame containing analysis results
     */
    DataFrame analyze(String column);

    /**
     * Calculates basic statistical measures for a column.
     * @param column Name of the column
     * @return Map containing statistical measures
     */
    Map<String, Double> stats(String column);

    /**
     * Calculates the mean of a column.
     * @param column Name of the column
     * @return Mean value
     */
    double mean(String column);

    /**
     * Calculates the median of a column.
     * @param column Name of the column
     * @return Median value
     */
    double median(String column);

    /**
     * Calculates the standard deviation of a column.
     * @param column Name of the column
     * @return Standard deviation value
     */
    double stdv(String column);

    /**
     * Calculates the variance of a column.
     * @param column Name of the column
     * @return Variance value
     */
    double vars(String column);

    /**
     * Calculates the skewness of a column.
     * @param column Name of the column
     * @return Skewness value
     */
    double skew(String column);

    /**
     * Calculates the kurtosis of a column.
     * @param column Name of the column
     * @return Kurtosis value
     */
    double kurt(String column);

    /**
     * Calculates the covariance between two columns.
     * @param col1 Name of the first column
     * @param col2 Name of the second column
     * @return Covariance value
     */
    double cov(String col1, String col2);

    /**
     * Performs general data cleaning operations.
     * @return Cleaned DataFrame
     */
    DataFrame clean();

    /**
     * Removes rows with null values.
     * @return DataFrame with null values removed
     */
    DataFrame dropNa();

    /**
     * Removes duplicate rows based on specified columns.
     * @param columns Column names to check for duplicates
     * @return DataFrame with duplicates removed
     */
    DataFrame dropDupes(String... columns);

    /**
     * Fills null values with a specified value.
     * @param value Value to fill nulls with
     * @return DataFrame with filled values
     */
    DataFrame fillNa(Object value);

    /**
     * Fills null values in specified columns with a given value.
     * @param value Value to fill nulls with
     * @param columns Columns to fill
     * @return DataFrame with filled values
     */
    DataFrame fillNaColumns(Object value, String... columns);

    /**
     * Converts column types according to the provided type map.
     * @param typeMap Map of column names to their target types
     * @return DataFrame with converted types
     */
    DataFrame convert(Map<String, Class<?>> typeMap);

    /**
     * Interpolates missing values using specified method.
     * @param method Interpolation method to use
     * @param columns Columns to interpolate
     * @return DataFrame with interpolated values
     */
    DataFrame interpolate(String method, String... columns);

    /**
     * Standardizes specified columns (z-score normalization).
     * @param columns Columns to standardize
     * @return DataFrame with standardized values
     */
    DataFrame standardize(String... columns);

    /**
     * Normalizes specified columns to [0,1] range.
     * @param columns Columns to normalize
     * @return DataFrame with normalized values
     */
    DataFrame normalize(String... columns);

    /**
     * Exports DataFrame to CSV file.
     * @param filePath Path where to save the CSV file
     * @throws Exception If there's an error writing the file
     */
    void toCsv(String filePath) throws Exception;

    /**
     * Exports DataFrame to JSON file.
     * @param filePath Path where to save the JSON file
     * @throws Exception If there's an error writing the file
     */
    void toJson(String filePath) throws Exception;

    /**
     * Exports DataFrame to Excel file.
     * @param filePath Path where to save the Excel file
     * @throws Exception If there's an error writing the file
     */
    void toExcel(String filePath) throws Exception;

    /**
     * Exports DataFrame to PowerBI format.
     * @param filePath Path where to save the PowerBI file
     * @throws Exception If there's an error writing the file
     */
    void toPowerBI(String filePath) throws Exception;

    /**
     * Exports DataFrame to HTML format.
     * @param filePath Path where to save the HTML file
     * @throws Exception If there's an error writing the file
     */
    void toHtml(String filePath) throws Exception;

    /**
     * Exports DataFrame to LaTeX format.
     * @param filePath Path where to save the LaTeX file
     * @throws Exception If there's an error writing the file
     */
    void toLatex(String filePath) throws Exception;

    /**
     * Calculates the sum of a column.
     * @param column Column name
     * @return Sum value
     */
    double sum(String column);

    /**
     * Calculates the average of a column.
     * @param column Column name
     * @return Average value
     */
    double avg(String column);

    /**
     * Counts non-null values in a column.
     * @param column Column name
     * @return Count of non-null values
     */
    long count(String column);

    /**
     * Finds the minimum value in a column.
     * @param column Column name
     * @return Minimum value
     */
    double min(String column);

    /**
     * Finds the maximum value in a column.
     * @param column Column name
     * @return Maximum value
     */
    double max(String column);

    /**
     * Provides descriptive statistics for a column.
     * @param column Column name
     * @return Map of descriptive statistics
     */
    Map<String, Object> describe(String column);

    /**
     * Calculates the quantile value for a column.
     * @param column Column name
     * @param q Quantile value (0-1)
     * @return Quantile value
     */
    double quantile(String column, double q);

    /**
     * Calculates the interquartile range for a column.
     * @param column Column name
     * @return IQR value
     */
    double iqr(String column);

    /**
     * Calculates frequency distribution for a column.
     * @param column Column name
     * @return Map of values to their frequencies
     */
    Map<Object, Long> frequency(String column);

    /**
     * Generates normal distribution samples.
     * @param size Number of samples
     * @param mean Mean of the distribution
     * @param std Standard deviation
     * @return Array of samples
     */
    double[] normalDist(int size, double mean, double std);

    /**
     * Calculates normal probability density function value.
     * @param x Input value
     * @param mean Mean of the distribution
     * @param std Standard deviation
     * @return PDF value
     */
    double normalPdf(double x, double mean, double std);

    /**
     * Calculates normal cumulative distribution function value.
     * @param x Input value
     * @param mean Mean of the distribution
     * @param std Standard deviation
     * @return CDF value
     */
    double normalCdf(double x, double mean, double std);

    /**
     * Generates binomial distribution samples.
     * @param trials Number of trials
     * @param prob Success probability
     * @param size Number of samples
     * @return Array of samples
     */
    int[] binomialDist(int trials, double prob, int size);

    /**
     * Generates Poisson distribution samples.
     * @param lambda Rate parameter
     * @param size Number of samples
     * @return Array of samples
     */
    double[] poissonDist(double lambda, int size);

    /**
     * Generates uniform distribution samples.
     * @param size Number of samples
     * @param min Minimum value
     * @param max Maximum value
     * @return Array of samples
     */
    double[] uniformDist(int size, double min, double max);

    /**
     * Calculates correlation between two columns.
     * @param col1 First column name
     * @param col2 Second column name
     * @return Correlation coefficient
     */
    double correl(String col1, String col2);

    /**
     * Performs simple linear regression.
     * @param xCol Independent variable column
     * @param yCol Dependent variable column
     * @return Array containing slope and intercept
     */
    double[] linearReg(String xCol, String yCol);

    /**
     * Calculates R-squared value for linear regression.
     * @param xCol Independent variable column
     * @param yCol Dependent variable column
     * @return R-squared value
     */
    double rsquared(String xCol, String yCol);

    /**
     * Performs multiple linear regression.
     * @param xCols Independent variable columns
     * @param yCol Dependent variable column
     * @return DataFrame with regression results
     */
    DataFrame multipleReg(String[] xCols, String yCol);

    /**
     * Performs polynomial regression.
     * @param xCol Independent variable column
     * @param yCol Dependent variable column
     * @param degree Polynomial degree
     * @return DataFrame with regression results
     */
    DataFrame polynomialReg(String xCol, String yCol, int degree);

    /**
     * Performs logistic regression.
     * @param xCol Independent variable column
     * @param yCol Dependent variable column
     * @return DataFrame with regression results
     */
    DataFrame logisticReg(String xCol, String yCol);

    /**
     * Performs t-test between two columns.
     * @param col1 First column name
     * @param col2 Second column name
     * @return Map containing test results
     */
    Map<String, Double> tTest(String col1, String col2);

    /**
     * Performs one-way ANOVA test.
     * @param columns Column names to compare
     * @return Map containing test results
     */
    Map<String, Double> anova(String... columns);

    /**
     * Performs chi-square test of independence.
     * @param col1 First column name
     * @param col2 Second column name
     * @return Map containing test results
     */
    Map<String, Double> chiSquare(String col1, String col2);

    /**
     * Performs Shapiro-Wilk normality test.
     * @param column Column name
     * @return Map containing test results
     */
    Map<String, Double> shapiroWilk(String column);

    /**
     * Performs Mann-Whitney U test.
     * @param col1 First column name
     * @param col2 Second column name
     * @return Map containing test results
     */
    Map<String, Double> mannWhitney(String col1, String col2);

    /**
     * Calculates Simple Moving Average.
     * @param data Input data array
     * @param window Window size
     * @return Array of SMA values
     */
    double[] sma(double[] data, int window);

    /**
     * Calculates Exponential Moving Average.
     * @param data Input data array
     * @param alpha Smoothing factor
     * @return Array of EMA values
     */
    double[] ema(double[] data, double alpha);

    /**
     * Forecasts future values using time series analysis.
     * @param timeCol Time column name
     * @param valueCol Value column name
     * @param periods Number of periods to forecast
     * @return DataFrame with forecasted values
     */
    DataFrame forecast(String timeCol, String valueCol, int periods);

    /**
     * Decomposes time series into components.
     * @param timeCol Time column name
     * @param valueCol Value column name
     * @return DataFrame with decomposition components
     */
    DataFrame decompose(String timeCol, String valueCol);

    /**
     * Performs seasonal adjustment on time series.
     * @param timeCol Time column name
     * @param valueCol Value column name
     * @return DataFrame with adjusted values
     */
    DataFrame seasonalAdjust(String timeCol, String valueCol);

    /**
     * Detects outliers in time series data.
     * @param timeCol Time column name
     * @param valueCol Value column name
     * @return DataFrame with outlier information
     */
    DataFrame detectOutliers(String timeCol, String valueCol);

    /**
     * Creates a pivot table from the DataFrame.
     * @param index Index column name
     * @param columns Column names for pivot
     * @param values Values column name
     * @return Pivoted DataFrame
     */
    DataFrame pivot(String index, String columns, String values);

    /**
     * Unpivots DataFrame from wide to long format.
     * @param idVars Columns to use as identifier variables
     * @param valueVars Columns to unpivot
     * @return Melted DataFrame
     */
    DataFrame melt(String[] idVars, String[] valueVars);

    /**
     * Creates dummy/indicator variables.
     * @param columns Columns to convert to dummy variables
     * @return DataFrame with dummy variables
     */
    DataFrame dummies(String... columns);

    /**
     * Bins continuous data into discrete intervals.
     * @param column Column to bin
     * @param bins Number of bins
     * @return DataFrame with binned data
     */
    DataFrame bin(String column, int bins);

    /**
     * Applies function over rolling window.
     * @param column Column name
     * @param window Window size
     * @param func Function to apply
     * @return DataFrame with rolling window calculations
     */
    DataFrame rollingWindow(String column, int window, String func);

    /**
     * Groups DataFrame by specified columns.
     * @param columns Columns to group by
     * @return Grouped DataFrame
     */
    DataFrame groupBy(String... columns);

    /**
     * Sorts DataFrame by specified columns.
     * @param columns Columns to sort by
     * @return Sorted DataFrame
     */
    DataFrame sort(String... columns);

/**
     * Selects specified columns from DataFrame.
     * @param columns Columns to select
     * @return DataFrame containing only the selected columns
     */
    DataFrame select(String... columns);

    /**
     * Creates a random sample of rows from the DataFrame.
     * @param n Number of rows to sample
     * @return DataFrame containing the sampled rows
     */
    DataFrame sample(int n);

    /**
     * Merges current DataFrame with another DataFrame.
     * @param other DataFrame to merge with
     * @param how Type of merge ('inner', 'outer', 'left', 'right')
     * @param on Columns to merge on
     * @return Merged DataFrame
     */
    DataFrame merge(DataFrame other, String how, String... on);

    /**
     * Concatenates current DataFrame with another DataFrame.
     * @param other DataFrame to concatenate
     * @param axis If true, concatenate along columns; if false, along rows
     * @return Concatenated DataFrame
     */
    DataFrame concat(DataFrame other, boolean axis);

    /**
     * Reshapes the DataFrame to specified dimensions.
     * @param rows Number of rows in reshaped DataFrame
     * @param cols Number of columns in reshaped DataFrame
     * @return Reshaped DataFrame
     */
    DataFrame reshape(int rows, int cols);

    /**
     * Performs quick exploratory data analysis on specified columns.
     * @param columns Columns to analyze
     * @return DataFrame containing analysis results including basic statistics,
     *         distribution information, and potential anomalies
     */
    DataFrame quickAnalysis(String... columns);

    /**
     * Generates a comprehensive analysis report for specified columns.
     * @param columns Columns to analyze
     * @return Map containing detailed analysis results including statistical tests,
     *         visualizations, and data quality metrics
     */
    Map<String, Object> fullReport(String... columns);

    /**
     * Performs time-based analysis on a datetime column and corresponding value column.
     * @param dateCol Column containing datetime values
     * @param valueCol Column containing values to analyze
     * @return DataFrame with time-based analysis results including trends,
     *         seasonality, and temporal patterns
     */
    DataFrame timeAnalysis(String dateCol, String valueCol);

    /**
     * Calculates correlation matrix for specified columns.
     * @param columns Columns to include in correlation analysis
     * @return DataFrame containing correlation matrix with correlation coefficients
     *         between all pairs of specified columns
     */
    DataFrame correlation(String... columns);

    /**
     * Analyzes missing values in the DataFrame.
     * @return DataFrame containing missing value analysis including counts,
     *         percentages, and patterns of missing data
     */
    DataFrame missingAnalysis();

    /**
     * Identifies and analyzes outliers in specified columns.
     * @param columns Columns to check for outliers
     * @return DataFrame containing outlier analysis results including identified
     *         outliers, their impact, and statistical justification
     */
    DataFrame outlierAnalysis(String... columns);
}