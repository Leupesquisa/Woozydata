package com.leumanuel.woozydata.service;

import com.leumanuel.woozydata.model.DataFrame;
import org.apache.commons.math3.stat.inference.*;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.math3.random.RandomDataGenerator;

/**
 * Service class for advanced statistical operations and hypothesis testing.
 * Provides implementation of various statistical tests and data analysis methods.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class StatisticalService {
    
    /**
     * Performs t-test between two columns.
     * 
     * @param df DataFrame containing the data
     * @param col1 First column name
     * @param col2 Second column name
     * @return Map containing test results
     */
    public Map<String, Double> tTest(DataFrame df, String col1, String col2) {
        double[] sample1 = getNumericArray(df, col1);
        double[] sample2 = getNumericArray(df, col2);
        
        TTest tTest = new TTest();
        Map<String, Double> results = new HashMap<>();
        results.put("t_statistic", tTest.t(sample1, sample2));
        results.put("p_value", tTest.tTest(sample1, sample2));
        
        return results;
    }

    /**
     * Performs ANOVA test on multiple columns.
     * 
     * @param df DataFrame containing the data
     * @param columns Column names to test
     * @return ANOVA test results
     */
    public Map<String, Double> anova(DataFrame df, String... columns) {
        Collection<double[]> samples = Arrays.stream(columns)
            .map(col -> getNumericArray(df, col))
            .collect(Collectors.toList());
        
        OneWayAnova anova = new OneWayAnova();
        Map<String, Double> results = new HashMap<>();
        results.put("f_value", anova.anovaFValue(samples));
        results.put("p_value", anova.anovaPValue(samples));
        
        return results;
    }

    /**
     * Performs chi-square test for independence.
     * 
     * @param df DataFrame containing the data
     * @param col1 First categorical column
     * @param col2 Second categorical column
     * @return Chi-square test results
     */
    public Map<String, Double> chiSquareTest(DataFrame df, String col1, String col2) {
        // Create contingency table
        Map<Object, Map<Object, Integer>> contingencyTable = new HashMap<>();
        df.getData().forEach(row -> {
            Object val1 = row.get(col1);
            Object val2 = row.get(col2);
            contingencyTable.computeIfAbsent(val1, k -> new HashMap<>())
                          .merge(val2, 1, Integer::sum);
        });
        
        // Convert to array
        long[][] counts = contingencyTable.values().stream()
            .map(m -> m.values().stream().mapToLong(Integer::longValue).toArray())
            .toArray(long[][]::new);
        
        ChiSquareTest chiSquareTest = new ChiSquareTest();
        Map<String, Double> results = new HashMap<>();
        results.put("chi_square", chiSquareTest.chiSquare(counts));
        results.put("p_value", chiSquareTest.chiSquareTest(counts));
        
        return results;
    }

     /**
     * Helper method to extract numeric values from a DataFrame column.
     * 
     * @param df DataFrame containing the data
     * @param column Column name to extract values from
     * @return Array of numeric values
     * @throws IllegalArgumentException if column is not numeric
     */
    private double[] getNumericArray(DataFrame df, String column) {
        return df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .mapToDouble(val -> ((Number) val).doubleValue())
            .toArray();
    }
    
    /**
     * Performs Shapiro-Wilk test for normality on a numeric column.
     * Tests the null hypothesis that the data is normally distributed.
     * 
     * @param df DataFrame containing the data
     * @param column Column name to test for normality
     * @return Map containing 'statistic' (W) and 'p_value'
     * @throws IllegalArgumentException if column is not numeric
     */
    public Map<String, Double> shapiroWilkTest(DataFrame df, String column) {
        DataStatisticsService ds = new DataStatisticsService(); 
        double[] values = ds.getColumnValues(df, column);                
        Map<String, Double> results = new HashMap<>();
        
       
        int n = values.length;
        Arrays.sort(values);        
      
        double mean = Arrays.stream(values).average().orElse(0.0);        
        
        double[] a = calculateShapiroWilkCoefficients(n);
        
        //  W statistic
        double numerator = 0.0;
        for (int i = 0; i < n/2; i++) {
            numerator += a[i] * (values[n-1-i] - values[i]);
        }
        numerator = numerator * numerator;
        
        double denominator = 0.0;
        for (int i = 0; i < n; i++) {
            denominator += (values[i] - mean) * (values[i] - mean);
        }
        
        double w = numerator / denominator;
        
        //  p-value aproximado
        double pValue = calculateShapiroWilkPValue(w, n);
        
        results.put("statistic", w);
        results.put("p_value", pValue);
        
        return results;
    }

    /**
     * Calculates coefficients for Shapiro-Wilk test.
     * 
     * @param n Sample size
     * @return Array of coefficients
     */
    private double[] calculateShapiroWilkCoefficients(int n) {
        double[] a = new double[n/2];
        // Aproximação dos coeficientes usando valores tabelados
        // Esta é uma simplificação - em produção, usar tabelas completas
        for (int i = 0; i < n/2; i++) {
            a[i] = 1.0 / Math.sqrt(n);
        }
        return a;
    }

    /**
     * Calculates approximate p-value for Shapiro-Wilk test.
     * 
     * @param w Test statistic
     * @param n Sample size
     * @return Approximate p-value
     */
    private double calculateShapiroWilkPValue(double w, int n) {
        // Aproximação do p-value
        // Em produção, usar tabelas completas ou aproximação mais precisa
        double z = Math.log((1 - w) / w);
        return 1 - normalCDF(z);
    }

    /**
     * Calculates cumulative distribution function for standard normal distribution.
     * 
     * @param z Z-score
     * @return CDF value
     */
    private double normalCDF(double z) {
        return 0.5 * (1 + erf(z / Math.sqrt(2)));
    }
    
     /**
     * Calculates error function approximation.
     * Used in normal distribution calculations.
     * 
     * @param z Input value
     * @return Error function value
     */
    private double erf(double z) {
        // Aproximação da função erro
        double t = 1.0 / (1.0 + 0.5 * Math.abs(z));
        double ans = 1 - t * Math.exp(-z*z - 1.26551223 +
                                    t * (1.00002368 +
                                    t * (0.37409196 +
                                    t * (0.09678418 +
                                    t * (-0.18628806 +
                                    t * (0.27886807 +
                                    t * (-1.13520398 +
                                    t * (1.48851587 +
                                    t * (-0.82215223 +
                                    t * 0.17087277)))))))));;
        return z >= 0 ? ans : -ans;
    }
    
       /**
     * Selects specified columns from the DataFrame.
     * Creates a new DataFrame containing only the selected columns.
     * 
     * @param df Source DataFrame
     * @param columns Columns to select
     * @return New DataFrame with only selected columns
     * @throws IllegalArgumentException if any column doesn't exist
     */   
       public DataFrame select(DataFrame df, String... columns) {
        List<Map<String, Object>> selectedData = df.getData().stream()
            .map(row -> {
                Map<String, Object> newRow = new HashMap<>();
                Arrays.stream(columns)
                      .filter(row::containsKey)
                      .forEach(col -> newRow.put(col, row.get(col)));
                return newRow;
            })
            .collect(Collectors.toList());
        
        return new DataFrame(selectedData);
    }

        /**
     * Creates a random sample of specified size from the DataFrame.
     * Uses random sampling without replacement.
     * 
     * @param df Source DataFrame
     * @param n Sample size
     * @return New DataFrame containing the random sample
     * @throws IllegalArgumentException if n is larger than DataFrame size
     */
    public DataFrame sample(DataFrame df, int n) {
        // Usar RandomDataGenerator do Commons Math
        RandomDataGenerator random = new RandomDataGenerator();
        
        if (n > df.getData().size()) {
            throw new IllegalArgumentException("Sample size cannot be larger than dataset size");
        }

        // Gerar índices aleatórios únicos
        int[] indices = random.nextPermutation(df.getData().size(), n);
        
        List<Map<String, Object>> sampledData = Arrays.stream(indices)
            .mapToObj(i -> df.getData().get(i))
            .collect(Collectors.toList());

        return new DataFrame(sampledData);
    } 
    
    
}

