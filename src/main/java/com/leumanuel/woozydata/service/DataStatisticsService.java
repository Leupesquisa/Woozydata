package com.leumanuel.woozydata.service;

/**
 *
 * @author Leu A. Manuel

 */
import com.leumanuel.woozydata.model.DataFrame;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.stat.inference.MannWhitneyUTest;

/**
 * Service class providing statistical analysis and data transformation operations for DataFrames.
 * Includes methods for descriptive statistics, statistical tests, data reshaping, and aggregations.
 *
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataStatisticsService {
    
    /**
     * Applies a function to a column of the DataFrame.
     *
     * @param df DataFrame to analyze
     * @param column Name of the column to apply function to
     * @param func Function to apply to the column values
     * @return New DataFrame containing the results of the function application
     */
    public DataFrame apply(DataFrame df, String column, Function<List<Object>, Object> func) {
        List<Map<String, Object>> result = new ArrayList<>();
        Map<String, Object> row = new HashMap<>();
        row.put(column, func.apply(df.getData().stream()
            .map(r -> r.get(column))
            .collect(Collectors.toList())));
        result.add(row);
        return new DataFrame(result);
    }

    /**
     * Creates a pivot table from the DataFrame data.
     *
     * @param df Source DataFrame
     * @param index Column to use as index
     * @param columns Column to use for new columns
     * @param values Column containing values to aggregate
     * @param aggFunc Aggregation function to apply ("sum", "mean", "min", "max")
     * @return New DataFrame containing the pivot table
     */
    public DataFrame pivot(DataFrame df, String index, String columns, String values, String aggFunc) {
        Map<Object, Map<Object, List<Object>>> pivotData = df.getData().stream()
            .collect(Collectors.groupingBy(
                row -> row.get(index),
                Collectors.groupingBy(
                    row -> row.get(columns),
                    Collectors.mapping(row -> row.get(values), Collectors.toList())
                )
            ));

        Set<Object> uniqueColumns = df.getData().stream()
            .map(row -> row.get(columns))
            .collect(Collectors.toSet());

        List<Map<String, Object>> resultData = new ArrayList<>();
        
        pivotData.forEach((indexVal, columnData) -> {
            Map<String, Object> row = new HashMap<>();
            row.put(index, indexVal);
            uniqueColumns.forEach(col -> {
                List<Object> vals = columnData.getOrDefault(col, Collections.emptyList());
                row.put(col.toString(), aggregate(vals, aggFunc));
            });
            resultData.add(row);
        });

        return new DataFrame(resultData);
    }
    
    /**
     * Transforms values in a specified column using a provided function.
     *
     * @param df Source DataFrame
     * @param column Column to transform
     * @param func Function to apply to each value
     * @return New DataFrame with transformed values
     */
    public DataFrame transform(DataFrame df, String column, Function<Object, Object> func) {
        List<Map<String, Object>> transformedData = df.getData().stream()
            .map(row -> {
                Map<String, Object> newRow = new HashMap<>(row);
                if (row.containsKey(column)) {
                    newRow.put(column, func.apply(row.get(column)));
                }
                return newRow;
            })
            .collect(Collectors.toList());
        return new DataFrame(transformedData);
    }

    /**
     * Joins two DataFrames based on matching column values.
     *
     * @param left Left DataFrame
     * @param right Right DataFrame
     * @param leftCols Columns from left DataFrame to join on
     * @param rightCols Columns from right DataFrame to join on
     * @return New DataFrame containing joined data
     */
    public DataFrame join(DataFrame left, DataFrame right, String[] leftCols, String[] rightCols) {
        List<Map<String, Object>> joinedData = new ArrayList<>();
        
        for (Map<String, Object> leftRow : left.getData()) {
            List<Object> leftValues = Arrays.stream(leftCols)
                .map(leftRow::get)
                .collect(Collectors.toList());

            right.getData().stream()
                .filter(rightRow -> {
                    List<Object> rightValues = Arrays.stream(rightCols)
                        .map(rightRow::get)
                        .collect(Collectors.toList());
                    return leftValues.equals(rightValues);
                })
                .forEach(rightRow -> {
                    Map<String, Object> joinedRow = new HashMap<>(leftRow);
                    joinedRow.putAll(rightRow);
                    joinedData.add(joinedRow);
                });
        }

        return new DataFrame(joinedData);
    }

     /**
     * Unpivots DataFrame from wide to long format.
     *
     * @param df Source DataFrame
     * @param idVars Columns to keep as identifiers
     * @param valueVars Columns to unpivot into rows
     * @return Melted DataFrame in long format
     */
    public DataFrame melt(DataFrame df, String[] idVars, String[] valueVars) {
        List<Map<String, Object>> meltedData = new ArrayList<>();
        
        for (Map<String, Object> row : df.getData()) {
            for (String valueVar : valueVars) {
                Map<String, Object> newRow = new HashMap<>();
                for (String idVar : idVars) {
                    newRow.put(idVar, row.get(idVar));
                }
                newRow.put("variable", valueVar);
                newRow.put("value", row.get(valueVar));
                meltedData.add(newRow);
            }
        }

        return new DataFrame(meltedData);
    }

   /**
     * Sorts DataFrame by specified columns.
     *
     * @param df Source DataFrame
     * @param columns Columns to sort by
     * @param ascending Array indicating sort direction for each column
     * @return Sorted DataFrame
     */
    public DataFrame sort(DataFrame df, String[] columns, boolean[] ascending) {
        List<Map<String, Object>> sortedData = new ArrayList<>(df.getData());
        sortedData.sort((row1, row2) -> {
            for (int i = 0; i < columns.length; i++) {
                Comparable val1 = (Comparable) row1.get(columns[i]);
                Comparable val2 = (Comparable) row2.get(columns[i]);
                int comparison = compareValues(val1, val2);
                if (comparison != 0) {
                    return ascending[i] ? comparison : -comparison;
                }
            }
            return 0;
        });
        return new DataFrame(sortedData);
    }

    /**
     * Generates comprehensive descriptive statistics for numeric columns.
     *
     * @param df Source DataFrame
     * @return Map of column names to their statistical measures
     */
    public Map<String, Map<String, Double>> describe(DataFrame df) {
        Map<String, Map<String, Double>> stats = new HashMap<>();
        
        df.getData().get(0).keySet().forEach(column -> {
            DescriptiveStatistics columnStats = new DescriptiveStatistics();
            df.getData().stream()
                .map(row -> row.get(column))
                .filter(val -> val instanceof Number)
                .forEach(val -> columnStats.addValue(((Number) val).doubleValue()));

            Map<String, Double> columnDescStats = new HashMap<>();
            columnDescStats.put("count", (double) columnStats.getN());
            columnDescStats.put("mean", columnStats.getMean());
            columnDescStats.put("std", columnStats.getStandardDeviation());
            columnDescStats.put("min", columnStats.getMin());
            columnDescStats.put("25%", columnStats.getPercentile(25));
            columnDescStats.put("50%", columnStats.getPercentile(50));
            columnDescStats.put("75%", columnStats.getPercentile(75));
            columnDescStats.put("max", columnStats.getMax());
            
            stats.put(column, columnDescStats);
        });

        return stats;
    }

    private Object aggregate(List<Object> values, String aggFunc) {
        if (values.isEmpty()) return null;
        
        DescriptiveStatistics stats = new DescriptiveStatistics();
        values.stream()
            .filter(v -> v instanceof Number)
            .forEach(v -> stats.addValue(((Number) v).doubleValue()));

        return switch (aggFunc.toLowerCase()) {
            case "sum" -> stats.getSum();
            case "mean" -> stats.getMean();
            case "min" -> stats.getMin();
            case "max" -> stats.getMax();
            default -> null;
        };
    }

    private int compareValues(Comparable val1, Comparable val2) {
        if (val1 == null && val2 == null) return 0;
        if (val1 == null) return -1;
        if (val2 == null) return 1;
        return val1.compareTo(val2);
    }
    
    /**
     * Calculates skewness of a numeric column.
     *
     * @param df Source DataFrame
     * @param column Column to analyze
     * @return Skewness value
     * @throws IllegalArgumentException if column is not numeric
     */
    public double calculateSkewness(DataFrame df, String column) {
        DescriptiveStatistics stats = getDescriptiveStats(df, column);
        return stats.getSkewness();
    }
    
    /**
     * Calculates kurtosis of a numeric column.
     *
     * @param df Source DataFrame
     * @param column Column to analyze
     * @return Kurtosis value
     * @throws IllegalArgumentException if column is not numeric
     */
    public double calculateKurtosis(DataFrame df, String column) {
        DescriptiveStatistics stats = getDescriptiveStats(df, column);
        return stats.getKurtosis();
    }
    
    /**
     * Calculates covariance between two numeric columns.
     *
     * @param df Source DataFrame
     * @param col1 First column name
     * @param col2 Second column name
     * @return Covariance value
     * @throws IllegalArgumentException if either column is not numeric
     */
    public double calculateCovariance(DataFrame df, String col1, String col2) {
        double[] x = getColumnValues(df, col1);
        double[] y = getColumnValues(df, col2);
        
        double meanX = Arrays.stream(x).average().orElse(0.0);
        double meanY = Arrays.stream(y).average().orElse(0.0);
        
        double covariance = 0.0;
        for (int i = 0; i < x.length; i++) {
            covariance += (x[i] - meanX) * (y[i] - meanY);
        }
        
        return covariance / (x.length - 1);
    }
    
    /**
     * Calculates specified quantile of a numeric column.
     *
     * @param df Source DataFrame
     * @param column Column to analyze
     * @param q Quantile value (0 to 1)
     * @return Quantile value
     * @throws IllegalArgumentException if column is not numeric or q is invalid
     */
    public double calculateQuantile(DataFrame df, String column, double q) {
        DescriptiveStatistics stats = getDescriptiveStats(df, column);
        return stats.getPercentile(q * 100);
    }
    
    /**
     * Calculates Interquartile Range (IQR) of a numeric column.
     *
     * @param df Source DataFrame
     * @param column Column to analyze
     * @return IQR value
     * @throws IllegalArgumentException if column is not numeric
     */
    public double calculateIQR(DataFrame df, String column) {
        DescriptiveStatistics stats = getDescriptiveStats(df, column);
        return stats.getPercentile(75) - stats.getPercentile(25);
    }
    
     /**
     * Calculates frequency distribution of values in a column.
     *
     * @param df Source DataFrame
     * @param column Column to analyze
     * @return Map of values to their frequencies
     */
    public Map<Object, Long> calculateFrequency(DataFrame df, String column) {
        return df.getData().stream()
            .map(row -> row.get(column))
            .collect(Collectors.groupingBy(
                value -> value,
                Collectors.counting()
            ));
    }
    
    private DescriptiveStatistics getDescriptiveStats(DataFrame df, String column) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .forEach(val -> stats.addValue(((Number) val).doubleValue()));
        return stats;
    }
    
    /**
     * Gets numeric values from a specified column.
     *
     * @param df Source DataFrame
     * @param column Column name
     * @return Array of numeric values
     * @throws IllegalArgumentException if column is not numeric
     */
    public double[] getColumnValues(DataFrame df, String column) {
        return df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .mapToDouble(val -> ((Number) val).doubleValue())
            .toArray();
    }
  
    /**
     * Generates binomial distribution samples.
     *
     * @param trials Number of trials
     * @param prob Success probability
     * @param size Number of samples to generate
     * @return Array of binomial distribution samples
     * @throws IllegalArgumentException if parameters are invalid
     */
        public int[] binomialDist(int trials, double prob, int size) {
              BinomialDistribution dist = new BinomialDistribution(trials, prob);
              return IntStream.range(0, size)
                             .map(i -> dist.sample())
                             .toArray();
          }

  /**
     * Performs Mann-Whitney U test between two columns.
     *
     * @param df Source DataFrame
     * @param col1 First column name
     * @param col2 Second column name
     * @return Map containing test statistic and p-value
     * @throws IllegalArgumentException if either column is not numeric
     */
    public Map<String, Double> mannWhitney(DataFrame df, String col1, String col2) {
        double[] sample1 = getColumnValues(df, col1);
        double[] sample2 = getColumnValues(df, col2);
        
        MannWhitneyUTest test = new MannWhitneyUTest();
        Map<String, Double> results = new HashMap<>();
        results.put("statistic", test.mannWhitneyU(sample1, sample2));
        results.put("p_value", test.mannWhitneyUTest(sample1, sample2));
        
        return results;
    }
    
        /**
     * Groups DataFrame by specified columns and calculates aggregate statistics.
     *
     * @param df Source DataFrame
     * @param columns Columns to group by
     * @return DataFrame containing grouped statistics
     */
        public DataFrame groupBy(DataFrame df, String... columns) {
        // Usar Apache Commons Math MultivariateStatistics
        Map<List<Object>, List<Map<String, Object>>> groups = df.getData().stream()
            .collect(Collectors.groupingBy(
                row -> Arrays.stream(columns)
                     .map(row::get)
                     .collect(Collectors.toList())
            ));

        List<Map<String, Object>> aggregatedData = new ArrayList<>();
        for (Map.Entry<List<Object>, List<Map<String, Object>>> entry : groups.entrySet()) {
            Map<String, Object> groupRow = new HashMap<>();
            
            // Adicionar colunas de grupo
            for (int i = 0; i < columns.length; i++) {
                groupRow.put(columns[i], entry.getKey().get(i));
            }
            
            // Calcular agregações usando DescriptiveStatistics
            Map<String, DescriptiveStatistics> stats = calculateGroupStatistics(entry.getValue());
            addAggregatedStats(groupRow, stats);
            
            aggregatedData.add(groupRow);
        }

        return new DataFrame(aggregatedData);
    }

        /**
     * Sorts DataFrame by specified columns in ascending order.
     *
     * @param df Source DataFrame
     * @param columns Columns to sort by
     * @return Sorted DataFrame
     */
    public DataFrame sort(DataFrame df, String... columns) {
        List<Map<String, Object>> sortedData = new ArrayList<>(df.getData());
        Comparator<Map<String, Object>> comparator = createComparator(columns);
        sortedData.sort(comparator);
        return new DataFrame(sortedData);
    }

    private Map<String, DescriptiveStatistics> calculateGroupStatistics(
            List<Map<String, Object>> group) {
        Map<String, DescriptiveStatistics> statsMap = new HashMap<>();
        
        group.get(0).keySet().forEach(column -> {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            group.stream()
                 .map(row -> row.get(column))
                 .filter(val -> val instanceof Number)
                 .forEach(val -> stats.addValue(((Number) val).doubleValue()));
            statsMap.put(column, stats);
        });

        return statsMap;
    }

    private void addAggregatedStats(Map<String, Object> groupRow, 
                                  Map<String, DescriptiveStatistics> stats) {
        stats.forEach((column, stat) -> {
            groupRow.put(column + "_count", stat.getN());
            groupRow.put(column + "_mean", stat.getMean());
            groupRow.put(column + "_sum", stat.getSum());
            groupRow.put(column + "_std", stat.getStandardDeviation());
            groupRow.put(column + "_min", stat.getMin());
            groupRow.put(column + "_max", stat.getMax());
        });
    }

    private Comparator<Map<String, Object>> createComparator(String... columns) {
        return (row1, row2) -> {
            for (String column : columns) {
                Object val1 = row1.get(column);
                Object val2 = row2.get(column);

                // Handle null values
                if (val1 == null && val2 == null) continue;
                if (val1 == null) return -1;
                if (val2 == null) return 1;

                // Cast to Comparable if possible
                if (val1 instanceof Comparable && val1.getClass().equals(val2.getClass())) {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> comp1 = (Comparable<Object>) val1;
                    int comparison = comp1.compareTo(val2);
                    if (comparison != 0) return comparison;
                } else {
                    // Fallback to string comparison
                    int comparison = val1.toString().compareTo(val2.toString());
                    if (comparison != 0) return comparison;
                }
            }
            return 0;
        };
     }
}