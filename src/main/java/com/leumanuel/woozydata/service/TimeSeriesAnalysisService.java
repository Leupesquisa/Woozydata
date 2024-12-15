package com.leumanuel.woozydata.service;

import com.leumanuel.woozydata.model.DataFrame;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.*;

/**
 * Service class for time series analysis and forecasting.
 * Provides methods for various time series operations including moving averages,
 * decomposition, forecasting, and outlier detection.
 *
 * @author Leu A. Manuel
 * @version 1.0
 */
public class TimeSeriesAnalysisService {

    /**
     * Calculates Simple Moving Average (SMA) for a time series.
     *
     * @param data Array of time series values
     * @param windowSize Size of the moving window
     * @return Array containing moving averages
     * @throws IllegalArgumentException if windowSize is larger than data length
     */
     public double[] simpleMovingAverage(double[] data, int windowSize) {
        DescriptiveStatistics stats = new DescriptiveStatistics();
        stats.setWindowSize(windowSize);

        double[] movingAverage = new double[data.length - windowSize + 1];
        for (int i = 0; i < data.length; i++) {
            stats.addValue(data[i]);
            if (i >= windowSize - 1) {
                movingAverage[i - windowSize + 1] = stats.getMean();
            }
        }
        return movingAverage;
    }

    /**
     * Fits an Autoregressive (AR) model to the time series data.
     *
     * @param data Array of time series values
     * @param lag Number of lagged terms to include
     * @return Array of AR coefficients
     * @throws IllegalArgumentException if lag is larger than data length
     */
    public double[] autoRegressiveModel(double[] data, int lag) {
        // Preparação dos dados para regressão AR
        double[][] laggedData = new double[data.length - lag][lag];
        double[] observations = new double[data.length - lag];

        // Cria a matriz de lags e o vetor de observações
        for (int i = lag; i < data.length; i++) {
            for (int j = 0; j < lag; j++) {
                laggedData[i - lag][j] = data[i - j - 1];
            }
            observations[i - lag] = data[i];
        }

        // Configuração e ajuste do modelo de regressão
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.newSampleData(observations, laggedData);

        return regression.estimateRegressionParameters();
    }


    /**
     * Fits a Moving Average (MA) model to the time series data.
     *
     * @param data Array of time series values
     * @param lag Number of lagged error terms
     * @return Array of MA coefficients
     * @throws IllegalArgumentException if lag is larger than data length
     */
    public double[] movingAverageModel(double[] data, int lag) {
        // Preparação dos dados para regressão MA
        double[][] laggedErrors = new double[data.length - lag][lag];
        double[] observations = new double[data.length - lag];

        // Cria a matriz de lags dos erros e o vetor de observações
        for (int i = lag; i < data.length; i++) {
            observations[i - lag] = data[i];
            for (int j = 0; j < lag; j++) {
                laggedErrors[i - lag][j] = data[i - j - 1] - observations[i - lag];
            }
        }

        // Configuração e ajuste do modelo de regressão
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.newSampleData(observations, laggedErrors);

        return regression.estimateRegressionParameters();
    }

 
    /**
     * Fits an ARMA (Autoregressive Moving Average) model.
     *
     * @param data Array of time series values
     * @param arLag AR component lag
     * @param maLag MA component lag
     * @return Array containing combined AR and MA coefficients
     */
    public double[] armaModel(double[] data, int arLag, int maLag) {
        double[] arCoefficients = autoRegressiveModel(data, arLag);
        double[] maCoefficients = movingAverageModel(data, maLag);

        return mergeArrays(arCoefficients, maCoefficients);
    }


    /**
     * Fits an ARIMA (Autoregressive Integrated Moving Average) model.
     *
     * @param data Array of time series values
     * @param arLag AR component lag
     * @param d Order of differencing
     * @param maLag MA component lag
     * @return Array containing model coefficients
     */
    public double[] arimaModel(double[] data, int arLag, int d, int maLag) {
        // Diferencia a série para tornar a série estacionária, se necessário
        double[] differencedData = data;
        for (int i = 0; i < d; i++) {
            differencedData = difference(differencedData);
        }

        return armaModel(differencedData, arLag, maLag);
    }

   
     /**
     * Calculates first difference of time series.
     *
     * @param data Array of time series values
     * @return Array of differenced values
     */
    private double[] difference(double[] data) {
        double[] differenced = new double[data.length - 1];
        for (int i = 1; i < data.length; i++) {
            differenced[i - 1] = data[i] - data[i - 1];
        }
        return differenced;
    }

    /**
     * Combines two coefficient arrays into one.
     *
     * @param arCoefficients AR coefficients
     * @param maCoefficients MA coefficients
     * @return Combined array of coefficients
     */
    private double[] mergeArrays(double[] arCoefficients, double[] maCoefficients) {
        double[] combined = Arrays.copyOf(arCoefficients, arCoefficients.length + maCoefficients.length);
        System.arraycopy(maCoefficients, 0, combined, arCoefficients.length, maCoefficients.length);
        return combined;
    }
    
/**
     * Calculates Exponential Moving Average (EMA).
     *
     * @param data Array of time series values
     * @param alpha Smoothing factor (0 &lt; alpha &lt; 1)
     * @return Array containing exponential moving averages
     * @throws IllegalArgumentException if alpha is not between 0 and 1
     */
    public double[] exponentialMovingAverage(double[] data, double alpha) {
        double[] ema = new double[data.length];
        ema[0] = data[0];
        
        for (int i = 1; i < data.length; i++) {
            ema[i] = alpha * data[i] + (1 - alpha) * ema[i-1];
        }
        
        return ema;
    }

    /**
     * Generates forecasts for future time periods.
     *
     * @param df DataFrame containing time series data
     * @param timeCol Column containing time values
     * @param valueCol Column containing series values
     * @param periods Number of periods to forecast
     * @return DataFrame containing forecasted values
     */
    public DataFrame forecast(DataFrame df, String timeCol, String valueCol, int periods) {
        double[] values = getColumnValues(df, valueCol);
        List<Map<String, Object>> forecastData = new ArrayList<>();
        
        // Calcular média móvel para previsão
        double[] sma = calculateSMA(values, Math.min(12, values.length));
        double lastSMA = sma[sma.length - 1];
        
        // Calcular tendência
        double trend = calculateTrend(values);
        
        // Calcular sazonalidade
        double[] seasonality = calculateSeasonality(values);
        
        // Gerar previsões
        for (int i = 0; i < periods; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put(timeCol, getNextTimePeriod(df, timeCol, i + 1));
            double forecast = lastSMA + trend * (i + 1);
            if (seasonality.length > 0) {
                forecast += seasonality[i % seasonality.length];
            }
            row.put(valueCol, forecast);
            forecastData.add(row);
        }
        
        return new DataFrame(forecastData);
    }

    /**
     * Decomposes time series into trend, seasonal, and residual components.
     *
     * @param df DataFrame containing time series data
     * @param timeCol Column containing time values
     * @param valueCol Column containing series values
     * @return DataFrame with decomposed components
     */
    public DataFrame decompose(DataFrame df, String timeCol, String valueCol) {
        double[] values = getColumnValues(df, valueCol);
        int period = findPeriod(values);
        
        // Calcular componentes
        double[] trend = calculateTrend(values, period);
        double[] seasonal = calculateSeasonalityComponent(values, trend, period);
        double[] residual = calculateResidual(values, trend, seasonal);
        
        List<Map<String, Object>> components = new ArrayList<>();
        
        for (int i = 0; i < values.length; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put(timeCol, df.getData().get(i).get(timeCol));
            row.put("original", values[i]);
            row.put("trend", trend[i]);
            row.put("seasonal", seasonal[i]);
            row.put("residual", residual[i]);
            components.add(row);
        }
        
        return new DataFrame(components);
    }

    /**
     * Removes seasonal component from time series.
     *
     * @param df DataFrame containing time series data
     * @param timeCol Column containing time values
     * @param valueCol Column containing series values
     * @return DataFrame with seasonally adjusted values
     */
    public DataFrame seasonalAdjustment(DataFrame df, String timeCol, String valueCol) {
        double[] values = getColumnValues(df, valueCol);
        int period = findPeriod(values);
        
        double[] trend = calculateTrend(values, period);
        double[] seasonal = calculateSeasonalityComponent(values, trend, period);
        
        List<Map<String, Object>> adjustedData = new ArrayList<>();
        
        for (int i = 0; i < values.length; i++) {
            Map<String, Object> row = new HashMap<>();
            row.put(timeCol, df.getData().get(i).get(timeCol));
            row.put(valueCol, values[i] - seasonal[i]);
            adjustedData.add(row);
        }
        
        return new DataFrame(adjustedData);
    }

    /**
     * Detects outliers in time series data using IQR method.
     *
     * @param df DataFrame containing time series data
     * @param timeCol Column containing time values
     * @param valueCol Column containing series values
     * @return DataFrame containing identified outliers
     */
    public DataFrame detectTimeSeriesOutliers(DataFrame df, String timeCol, String valueCol) {
        double[] values = getColumnValues(df, valueCol);
        List<Map<String, Object>> outliers = new ArrayList<>();
        
        // Calcular limites usando IQR
        double q1 = calculateQuantile(values, 0.25);
        double q3 = calculateQuantile(values, 0.75);
        double iqr = q3 - q1;
        double lowerBound = q1 - 1.5 * iqr;
        double upperBound = q3 + 1.5 * iqr;
        
        for (int i = 0; i < values.length; i++) {
            if (values[i] < lowerBound || values[i] > upperBound) {
                Map<String, Object> outlier = new HashMap<>();
                outlier.put(timeCol, df.getData().get(i).get(timeCol));
                outlier.put(valueCol, values[i]);
                outlier.put("lower_bound", lowerBound);
                outlier.put("upper_bound", upperBound);
                outliers.add(outlier);
            }
        }
        
        return new DataFrame(outliers);
    }

    /**
     * Extracts numeric values from DataFrame column.
     *
     * @param df Source DataFrame
     * @param column Column name
     * @return Array of numeric values
     */
    private double[] getColumnValues(DataFrame df, String column) {
        return df.getData().stream()
            .map(row -> row.get(column))
            .filter(val -> val instanceof Number)
            .mapToDouble(val -> ((Number) val).doubleValue())
            .toArray();
    }

    private double[] calculateSMA(double[] data, int window) {
        double[] sma = new double[data.length - window + 1];
        for (int i = 0; i < sma.length; i++) {
            double sum = 0;
            for (int j = 0; j < window; j++) {
                sum += data[i + j];
            }
            sma[i] = sum / window;
        }
        return sma;
    }

 
    private double calculateTrend(double[] values) {
        int n = values.length;
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        
        for (int i = 0; i < n; i++) {
            sumX += i;
            sumY += values[i];
            sumXY += i * values[i];
            sumX2 += i * i;
        }
        
        return (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    }

       /**
     * Calculates trend component using moving average.
     *
     * @param values Time series values
     * @param period Seasonal period
     * @return Array containing trend values
     */
    private double[] calculateTrend(double[] values, int period) {
        double[] trend = new double[values.length];
        int halfPeriod = period / 2;
        
        for (int i = 0; i < values.length; i++) {
            int start = Math.max(0, i - halfPeriod);
            int end = Math.min(values.length, i + halfPeriod + 1);
            double sum = 0;
            int count = 0;
            
            for (int j = start; j < end; j++) {
                sum += values[j];
                count++;
            }
            
            trend[i] = sum / count;
        }
        
        return trend;
    }

    /**
     * Calculates seasonal indices.
     *
     * @param values Time series values
     * @return Array containing seasonal indices
     */
    private double[] calculateSeasonality(double[] values) {
        int period = findPeriod(values);
        if (period == 0) return new double[0];
        
        double[] seasonal = new double[period];
        int cycles = values.length / period;
        
        for (int i = 0; i < period; i++) {
            double sum = 0;
            for (int j = 0; j < cycles; j++) {
                if (i + j * period < values.length) {
                    sum += values[i + j * period];
                }
            }
            seasonal[i] = sum / cycles;
        }
        
        // Centralizar a sazonalidade
        double mean = Arrays.stream(seasonal).average().orElse(0);
        for (int i = 0; i < seasonal.length; i++) {
            seasonal[i] -= mean;
        }
        
        return seasonal;
    }

    private double[] calculateSeasonalityComponent(double[] values, double[] trend, int period) {
        double[] seasonal = new double[values.length];
        double[] meanSeasonal = new double[period];
        int cycles = values.length / period;
        
        for (int i = 0; i < period; i++) {
            double sum = 0;
            int count = 0;
            for (int j = 0; j < cycles; j++) {
                int idx = i + j * period;
                if (idx < values.length) {
                    sum += values[idx] - trend[idx];
                    count++;
                }
            }
            meanSeasonal[i] = sum / count;
        }
        
        for (int i = 0; i < values.length; i++) {
            seasonal[i] = meanSeasonal[i % period];
        }
        
        return seasonal;
    }

    private double[] calculateResidual(double[] values, double[] trend, double[] seasonal) {
        double[] residual = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            residual[i] = values[i] - trend[i] - seasonal[i];
        }
        return residual;
    }

     /**
     * Finds optimal seasonal period in time series.
     *
     * @param values Time series values
     * @return Estimated seasonal period
     */
    private int findPeriod(double[] values) {
        // Implementação simplificada - em produção, usar análise espectral ou autocorrelação
        int maxPeriod = values.length / 2;
        int bestPeriod = 0;
        double minVariance = Double.MAX_VALUE;
        
        for (int p = 2; p < maxPeriod; p++) {
            double variance = calculatePeriodVariance(values, p);
            if (variance < minVariance) {
                minVariance = variance;
                bestPeriod = p;
            }
        }
        
        return bestPeriod;
    }

    private double calculatePeriodVariance(double[] values, int period) {
        double[] means = new double[period];
        int[] counts = new int[period];
        
        for (int i = 0; i < values.length; i++) {
            int idx = i % period;
            means[idx] += values[i];
            counts[idx]++;
        }
        
        for (int i = 0; i < period; i++) {
            means[i] /= counts[i];
        }
        
        double variance = 0;
        for (int i = 0; i < values.length; i++) {
            double diff = values[i] - means[i % period];
            variance += diff * diff;
        }
        
        return variance / values.length;
    }

    /**
     * Calculates quantile value.
     *
     * @param values Array of numeric values
     * @param q Quantile (0 to 1)
     * @return Quantile value
     */
    private double calculateQuantile(double[] values, double q) {
        double[] sorted = Arrays.copyOf(values, values.length);
        Arrays.sort(sorted);
        int index = (int) Math.round(q * (sorted.length - 1));
        return sorted[index];
    }

    private Object getNextTimePeriod(DataFrame df, String timeCol, int offset) {
        // Implementação simplificada - assumindo valores numéricos ou datas
        Object lastTime = df.getData().get(df.getData().size() - 1).get(timeCol);
        if (lastTime instanceof Number) {
            return ((Number) lastTime).doubleValue() + offset;
        }
        // Para outros tipos, retornar o último valor
        return lastTime;
    }
}

