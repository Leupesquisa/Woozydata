package com.leumanuel.woozydata.service;

import com.leumanuel.woozydata.model.DataFrame;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.Map;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

/**
 * Service class for regression analysis and correlation calculations.
 * Provides methods for various types of regression models and statistical correlations.
 *
 * @author Leu A. Manuel
 * @version 1.0
 */
public class RegressionService {

    /**
     * Calculates Pearson correlation coefficient between two columns.
     *
     * @param dataFrame DataFrame containing the data
     * @param column1 Name of first column
     * @param column2 Name of second column
     * @return Pearson correlation coefficient
     * @throws IllegalArgumentException if columns are not numeric
     */
    public double calculatePearsonCorrelation(DataFrame dataFrame, String column1, String column2) {
        double[] values1 = dataFrame.getData().stream()
                .mapToDouble(row -> ((Number) row.get(column1)).doubleValue())
                .toArray();
        double[] values2 = dataFrame.getData().stream()
                .mapToDouble(row -> ((Number) row.get(column2)).doubleValue())
                .toArray();
        return new PearsonsCorrelation().correlation(values1, values2);
    }

    /**
     * Performs simple linear regression between two columns.
     * Returns array containing [slope, intercept].
     *
     * @param dataFrame DataFrame containing the data
     * @param xColumn Independent variable column name
     * @param yColumn Dependent variable column name
     * @return double array where [0] = slope, [1] = intercept
     * @throws IllegalArgumentException if columns are not numeric
     */
    public double[] simpleLinearRegression(DataFrame dataFrame, String xColumn, String yColumn) {
        SimpleRegression regression = new SimpleRegression();
        for (Map<String, Object> row : dataFrame.getData()) {
            double x = ((Number) row.get(xColumn)).doubleValue();
            double y = ((Number) row.get(yColumn)).doubleValue();
            regression.addData(x, y);
        }
        return new double[]{regression.getSlope(), regression.getIntercept()};
    }
    
    /**
     * Calculates R-squared (coefficient of determination) for linear regression.
     *
     * @param df DataFrame containing the data
     * @param xCol Independent variable column name
     * @param yCol Dependent variable column name
     * @return R-squared value
     * @throws IllegalArgumentException if columns are not numeric
     */
    public double calculateRSquared(DataFrame df, String xCol, String yCol) {
        double[] x = getColumnValues(df, xCol);
        double[] y = getColumnValues(df, yCol);
        
        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < x.length; i++) {
            regression.addData(x[i], y[i]);
        }
        
        return regression.getRSquare();
    }

     /**
     * Performs multiple linear regression analysis.
     *
     * @param df DataFrame containing the data
     * @param xCols Array of independent variable column names
     * @param yCol Dependent variable column name
     * @return DataFrame containing regression coefficients
     * @throws IllegalArgumentException if any column is not numeric
     */
    public DataFrame multipleRegression(DataFrame df, String[] xCols, String yCol) {
        double[][] x = new double[df.getData().size()][xCols.length];
        double[] y = getColumnValues(df, yCol);

        // Preparar matriz X
        for (int i = 0; i < xCols.length; i++) {
            double[] colValues = getColumnValues(df, xCols[i]);
            for (int j = 0; j < colValues.length; j++) {
                x[j][i] = colValues[j];
            }
        }

        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.newSampleData(y, x);


        List<Map<String, Object>> results = new ArrayList<>();
        Map<String, Object> coefficients = new HashMap<>();
        double[] params = regression.estimateRegressionParameters();
        
        coefficients.put("intercept", params[0]);
        for (int i = 0; i < xCols.length; i++) {
            coefficients.put(xCols[i], params[i + 1]);
        }
        results.add(coefficients);

        return new DataFrame(results);
    }

    /**
     * Performs polynomial regression analysis.
     *
     * @param df DataFrame containing the data
     * @param xCol Independent variable column name
     * @param yCol Dependent variable column name
     * @param degree Degree of polynomial
     * @return DataFrame containing polynomial coefficients
     * @throws IllegalArgumentException if columns are not numeric or degree is invalid
     */
    public DataFrame polynomialRegression(DataFrame df, String xCol, String yCol, int degree) {
        double[] x = getColumnValues(df, xCol);
        double[] y = getColumnValues(df, yCol);

        PolynomialCurveFitter fitter = PolynomialCurveFitter.create(degree);
        WeightedObservedPoints points = new WeightedObservedPoints();
        
        for (int i = 0; i < x.length; i++) {
            points.add(x[i], y[i]);
        }

        double[] coefficients = fitter.fit(points.toList());
        
        List<Map<String, Object>> results = new ArrayList<>();
        Map<String, Object> coeffMap = new HashMap<>();
        for (int i = 0; i < coefficients.length; i++) {
            coeffMap.put("degree_" + i, coefficients[i]);
        }
        results.add(coeffMap);

        return new DataFrame(results);
    }

    /**
     * Performs logistic regression analysis for binary classification.
     *
     * @param df DataFrame containing the data
     * @param xCol Independent variable column name
     * @param yCol Dependent variable column name (should contain binary values)
     * @return DataFrame containing logistic regression coefficients
     * @throws IllegalArgumentException if columns are not numeric or y is not binary
     */
    public DataFrame logisticRegression(DataFrame df, String xCol, String yCol) {
        double[] x = getColumnValues(df, xCol);
        double[] y = getColumnValues(df, yCol);

        // Implementação simplificada de regressão logística
        LogisticRegression logistic = new LogisticRegression(x, y);
        double[] coefficients = logistic.fit();

        List<Map<String, Object>> results = new ArrayList<>();
        Map<String, Object> coeffMap = new HashMap<>();
        coeffMap.put("intercept", coefficients[0]);
        coeffMap.put("coefficient", coefficients[1]);
        results.add(coeffMap);

        return new DataFrame(results);
    }
    
    /**
     * Extracts numeric values from a specified column.
     *
     * @param df DataFrame containing the data
     * @param column Column name to extract values from
     * @return Array of numeric values
     * @throws IllegalArgumentException if column is not numeric
     */
    private double[] getColumnValues(DataFrame df, String column) {
        return df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .mapToDouble(val -> ((Number) val).doubleValue())
            .toArray();
    }

    /**
 * Internal helper class for logistic regression implementation.
 * Uses gradient descent to find optimal parameters.
 */
    private static class LogisticRegression {
        private final double[] x;
        private final double[] y;
        private static final int MAX_ITERATIONS = 100;
        private static final double LEARNING_RATE = 0.01;

            /**
         * Creates a new logistic regression instance.
         *
         * @param x Array of independent variable values
         * @param y Array of dependent variable values (binary)
         */
        public LogisticRegression(double[] x, double[] y) {
            this.x = x;
            this.y = y;
        }

            /**
         * Fits the logistic regression model using gradient descent.
         *
         * @return Array containing [intercept, coefficient]
         */
        public double[] fit() {
            double b0 = 0, b1 = 0;
            
            for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
                double[] predictions = predict(b0, b1);
                double[] errors = new double[y.length];
                
                for (int i = 0; i < y.length; i++) {
                    errors[i] = y[i] - predictions[i];
                }
                
                double db0 = 0, db1 = 0;
                for (int i = 0; i < y.length; i++) {
                    db0 += errors[i];
                    db1 += errors[i] * x[i];
                }
                
                b0 += LEARNING_RATE * db0 / y.length;
                b1 += LEARNING_RATE * db1 / y.length;
            }
            
            return new double[]{b0, b1};
        }

            /**
        * Makes predictions using current model parameters.
        *
        * @param b0 Intercept term
        * @param b1 Coefficient term
        * @return Array of predicted probabilities
        */
        private double[] predict(double b0, double b1) {
            double[] predictions = new double[x.length];
            for (int i = 0; i < x.length; i++) {
                predictions[i] = sigmoid(b0 + b1 * x[i]);
            }
            return predictions;
        }

            /**
         * Applies sigmoid function to input.
         *
         * @param z Input value
         * @return Sigmoid function output (between 0 and 1)
         */
        private double sigmoid(double z) {
            return 1.0 / (1.0 + Math.exp(-z));
        }
    }
}
