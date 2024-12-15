package com.leumanuel.woozydata.service;

import com.leumanuel.woozydata.model.DataFrame;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.analysis.interpolation.LinearInterpolator;
import org.apache.commons.math3.analysis.interpolation.LoessInterpolator;
import org.apache.commons.math3.analysis.interpolation.NevilleInterpolator;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;
import org.apache.commons.math3.analysis.interpolation.UnivariateInterpolator;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.fitting.WeightedObservedPoint;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

/**
 * Service class responsible for data cleaning and transformation operations on DataFrames.
 * Provides methods for handling missing values, removing duplicates, standardizing data,
 * and performing various data cleaning tasks.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataCleaningService {
    
    /**
     * Removes all rows containing missing values from the DataFrame.
     * Missing values are identified as null, empty strings, "null", or "nan" (case insensitive).
     *
     * @param df the DataFrame to clean
     * @return new DataFrame with rows containing missing values removed
     * @throws IllegalArgumentException if df is null
     */
    public DataFrame dropNa(DataFrame df) {
        List<Map<String, Object>> cleanData = df.getData().stream()
            .filter(row -> row.values().stream().noneMatch(this::isMissing))
            .collect(Collectors.toList());
        return new DataFrame(cleanData);
    }

    /**
     * Replaces all missing values in the DataFrame with a specified value.
     * Missing values are identified as null, empty strings, "null", or "nan" (case insensitive).
     *
     * @param df the DataFrame to process
     * @param value the value to use for replacing missing values
     * @return new DataFrame with missing values replaced
     * @throws IllegalArgumentException if df is null
     */
    public DataFrame fillNa(DataFrame df, Object value) {
        List<Map<String, Object>> filledData = df.getData().stream()
            .map(row -> {
                Map<String, Object> newRow = new HashMap<>(row);
                newRow.replaceAll((k, v) -> v == null ? value : v);
                return newRow;
            })
            .collect(Collectors.toList());
        return new DataFrame(filledData);
    }
    
    
    /**
     * Replaces missing values in specified columns with a given value.
     *
     * @param df the DataFrame to process
     * @param value the value to use for replacing missing values
     * @param columns array of column names where missing values should be replaced
     * @return new DataFrame with missing values replaced in specified columns
     * @throws IllegalArgumentException if df is null or if any specified column doesn't exist
     */
    public DataFrame fillNaColumns(DataFrame df, Object value, String... columns) {
        List<Map<String, Object>> filledData = df.getData().stream()
            .map(row -> {
                Map<String, Object> newRow = new HashMap<>(row);
                for (String column : columns) {
                    if (newRow.get(column) == null) {
                        newRow.put(column, value);
                    }
                }
                return newRow;
            })
            .collect(Collectors.toList());
        return new DataFrame(filledData);
    }


    /**
     * Removes duplicate rows from the DataFrame based on specified columns.
     * If no columns are specified, checks all columns for duplicates.
     *
     * @param df the DataFrame to process
     * @param columns array of column names to check for duplicates
     * @return new DataFrame with duplicate rows removed
     * @throws IllegalArgumentException if df is null or if any specified column doesn't exist
     */
    public DataFrame dropDuplicates(DataFrame df, String... columns) {
        Set<List<Object>> seen = new HashSet<>();
        List<Map<String, Object>> uniqueData = df.getData().stream()
            .filter(row -> {
                List<Object> values = Arrays.stream(columns)
                    .map(row::get)
                    .collect(Collectors.toList());
                return seen.add(values);
            })
            .collect(Collectors.toList());
        return new DataFrame(uniqueData);
    }

    /**
     * Replaces all occurrences of a specific value with a new value throughout the DataFrame.
     *
     * @param df the DataFrame to process
     * @param oldValue the value to be replaced
     * @param newValue the replacement value
     * @return new DataFrame with values replaced
     * @throws IllegalArgumentException if df is null
     */
    public DataFrame replace(DataFrame df, Object oldValue, Object newValue) {
        List<Map<String, Object>> replacedData = df.getData().stream()
            .map(row -> {
                Map<String, Object> newRow = new HashMap<>(row);
                newRow.replaceAll((k, v) -> Objects.equals(v, oldValue) ? newValue : v);
                return newRow;
            })
            .collect(Collectors.toList());
        return new DataFrame(replacedData);
    }

    /**
     * Creates a boolean mask indicating missing values in the DataFrame.
     *
     * @param df the DataFrame to analyze
     * @return new DataFrame containing boolean values (true for missing values)
     * @throws IllegalArgumentException if df is null
     */
    public DataFrame isna(DataFrame df) {
        List<Map<String, Object>> nullMask = df.getData().stream()
            .map(row -> {
                Map<String, Object> mask = new HashMap<>();
                row.forEach((k, v) -> mask.put(k, isMissing(v)));
                return mask;
            })
            .collect(Collectors.toList());
        return new DataFrame(nullMask);
    }

    /**
     * Creates a boolean mask indicating non-missing values in the DataFrame.
     *
     * @param df the DataFrame to analyze
     * @return new DataFrame containing boolean values (true for non-missing values)
     * @throws IllegalArgumentException if df is null
     */
    public DataFrame notna(DataFrame df) {
        List<Map<String, Object>> notNullMask = df.getData().stream()
            .map(row -> {
                Map<String, Object> mask = new HashMap<>();
                row.forEach((k, v) -> mask.put(k, !isMissing(v)));
                return mask;
            })
            .collect(Collectors.toList());
        return new DataFrame(notNullMask);
    }

    /**
     * Converts columns to specified data types.
     *
     * @param df the DataFrame to process
     * @param typeMap map of column names to their target Java types
     * @return new DataFrame with converted column types
     * @throws IllegalArgumentException if df is null or if conversion fails
     */
    public DataFrame astype(DataFrame df, Map<String, Class<?>> typeMap) {
        List<Map<String, Object>> castedData = df.getData().stream()
            .map(row -> {
                Map<String, Object> newRow = new HashMap<>(row);
                typeMap.forEach((col, type) -> {
                    if (newRow.containsKey(col)) {
                        newRow.put(col, castValue(newRow.get(col), type));
                    }
                });
                return newRow;
            })
            .collect(Collectors.toList());
        return new DataFrame(castedData);
    }

    /**
     * Groups DataFrame by specified columns.
     *
     * @param df the DataFrame to group
     * @param columns array of column names to group by
     * @return Map of group keys to corresponding DataFrames
     * @throws IllegalArgumentException if df is null or if any specified column doesn't exist
     */
    public Map<List<Object>, DataFrame> groupBy(DataFrame df, String... columns) {
        return df.getData().stream()
            .collect(Collectors.groupingBy(
                row -> Arrays.stream(columns)
                    .map(row::get)
                    .collect(Collectors.toList()),
                Collectors.collectingAndThen(
                    Collectors.toList(),
                    DataFrame::new
                )
            ));
    }

    private boolean isMissing(Object value) {
        return value == null || value.toString().isEmpty() || 
               value.toString().equalsIgnoreCase("null") || 
               value.toString().equalsIgnoreCase("nan");
    }


    private Object castValue(Object value, Class<?> type) {
        if (value == null) return null;
        try {
            if (type == Integer.class) return Integer.parseInt(value.toString());
            if (type == Double.class) return Double.parseDouble(value.toString());
            if (type == Boolean.class) return Boolean.parseBoolean(value.toString());
            return type.cast(value);
        } catch (Exception e) {
            return null;
        }
    }

     /**
     * Performs comprehensive data cleaning including removing nulls, duplicates, and filling remaining nulls.
     *
     * @param df the DataFrame to clean
     * @return cleaned DataFrame
     * @throws IllegalArgumentException if df is null
     */    
    public DataFrame clean(DataFrame df) {
        
        DataFrame withoutNulls = dropNa(df);
        String[] allColumns = df.getData().get(0).keySet().toArray(new String[0]);
        DataFrame withoutDuplicates = dropDuplicates(withoutNulls, allColumns);
        return fillNa(withoutDuplicates, 0);
    }
    
 
/**
     * Standardizes specified numeric columns using z-score normalization (x - mean) / std.
     *
     * @param df the DataFrame to process
     * @param columns array of column names to standardize
     * @return new DataFrame with standardized columns
     * @throws IllegalArgumentException if df is null or if any specified column isn't numeric
     */
    public DataFrame standardize(DataFrame df, String... columns) {
        Map<String, StandardizationParams> params = calculateStandardizationParams(df, columns);
        List<Map<String, Object>> standardizedData = df.getData().stream()
            .map(row -> standardizeRow(row, params))
            .collect(Collectors.toList());
        return new DataFrame(standardizedData);
    }


    /**
     * Normalizes specified numeric columns to range [0,1].
     *
     * @param df the DataFrame to process
     * @param columns array of column names to normalize
     * @return new DataFrame with normalized columns
     * @throws IllegalArgumentException if df is null or if any specified column isn't numeric
     */
    public DataFrame normalize(DataFrame df, String... columns) {
        DataStatisticsService ds = new DataStatisticsService();
        List<Map<String, Object>> normalizedData = new ArrayList<>();
        
        for (String column : columns) {
            double[] values = ds.getColumnValues(df, column);
            double min = Arrays.stream(values).min().orElse(0);
            double max = Arrays.stream(values).max().orElse(1);
            double range = max - min;
            
            for (Map<String, Object> row : df.getData()) {
                Map<String, Object> newRow = new HashMap<>(row);
                Object value = row.get(column);
                if (value instanceof Number) {
                    double originalValue = ((Number) value).doubleValue();
                    newRow.put(column, (originalValue - min) / range);
                }
                normalizedData.add(newRow);
            }
        }
        
        return new DataFrame(normalizedData);
    }

    
    private Map<String, StandardizationParams> calculateStandardizationParams(DataFrame df, String[] columns) {
        Map<String, StandardizationParams> params = new HashMap<>();
        
        for (String column : columns) {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            
            df.getData().stream()
                .map(row -> row.get(column))
                .filter(val -> val instanceof Number)
                .forEach(val -> stats.addValue(((Number) val).doubleValue()));
            
            params.put(column, new StandardizationParams(
                stats.getMean(),
                stats.getStandardDeviation()
            ));
        }
        
        return params;
    }

    private Map<String, Object> standardizeRow(Map<String, Object> row, 
                                             Map<String, StandardizationParams> params) {
        Map<String, Object> standardizedRow = new HashMap<>(row);
        
        params.forEach((column, param) -> {
            Object value = row.get(column);
            if (value instanceof Number) {
                double standardized = (((Number) value).doubleValue() - param.mean) / param.std;
                standardizedRow.put(column, standardized);
            }
        });
        
        return standardizedRow;
    }

    private Map<String, NormalizationParams> calculateNormalizationParams(DataFrame df, String[] columns) {
        Map<String, NormalizationParams> params = new HashMap<>();
        
        for (String column : columns) {
            DescriptiveStatistics stats = new DescriptiveStatistics();
            
            df.getData().stream()
                .map(row -> row.get(column))
                .filter(val -> val instanceof Number)
                .forEach(val -> stats.addValue(((Number) val).doubleValue()));
            
            params.put(column, new NormalizationParams(
                stats.getMin(),
                stats.getMax()
            ));
        }
        
        return params;
    }

    private Map<String, Object> normalizeRow(Map<String, Object> row,
                                           Map<String, NormalizationParams> params) {
        Map<String, Object> normalizedRow = new HashMap<>(row);
        
        params.forEach((column, param) -> {
            Object value = row.get(column);
            if (value instanceof Number) {
                double normalized = (((Number) value).doubleValue() - param.min) / (param.max - param.min);
                normalizedRow.put(column, normalized);
            }
        });
        
        return normalizedRow;
    }

    private static class StandardizationParams {
        final double mean;
        final double std;
        
        StandardizationParams(double mean, double std) {
            this.mean = mean;
            this.std = std != 0 ? std : 1.0; 
        }
    }

    private static class NormalizationParams {
        final double min;
        final double max;
        
        NormalizationParams(double min, double max) {
            this.min = min;
            this.max = max != min ? max : min + 1.0; 
        }
    }
    
    
           
 
    /**
     * Calculates basic statistics for a specified column.
     *
     * @param df the DataFrame to analyze
     * @param column the column name to analyze
     * @return Map containing statistics (mean, std, min, max, median)
     * @throws IllegalArgumentException if df is null or column isn't numeric
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
     * Converts column data types according to the provided type map.
     *
     * @param df the DataFrame to process
     * @param typeMap map of column names to target types
     * @return new DataFrame with converted types
     * @throws IllegalArgumentException if conversion fails or type is unsupported
     */
    public DataFrame convert(DataFrame df, Map<String, Class<?>> typeMap) {
        List<Map<String, Object>> convertedData = new ArrayList<>();
        
        for (Map<String, Object> row : df.getData()) {
            Map<String, Object> convertedRow = new HashMap<>(row);
            
            for (Map.Entry<String, Class<?>> entry : typeMap.entrySet()) {
                String column = entry.getKey();
                Class<?> targetType = entry.getValue();
                
                if (convertedRow.containsKey(column)) {
                    Object value = convertedRow.get(column);
                    convertedRow.put(column, convertValue(value, targetType));
                }
            }
            
            convertedData.add(convertedRow);
        }
        
        return new DataFrame(convertedData);
    }

   /**
     * Converts column data types according to the provided type map.
     *
     * @param df the DataFrame to process
     * @param typeMap map of column names to target types
     * @return new DataFrame with converted types
     * @throws IllegalArgumentException if conversion fails or type is unsupported
     */
    private Object convertValue(Object value, Class<?> targetType) {
        if (value == null) return null;
        
        try {
            if (targetType == Integer.class) {
                return value instanceof Number ? 
                    ((Number) value).intValue() : 
                    Integer.parseInt(value.toString());
            }
            else if (targetType == Double.class) {
                return value instanceof Number ? 
                    ((Number) value).doubleValue() : 
                    Double.parseDouble(value.toString());
            }
            else if (targetType == Long.class) {
                return value instanceof Number ? 
                    ((Number) value).longValue() : 
                    Long.parseLong(value.toString());
            }
            else if (targetType == Boolean.class) {
                return value instanceof Boolean ? 
                    value : 
                    Boolean.parseBoolean(value.toString());
            }
            else if (targetType == String.class) {
                return value.toString();
            }
            else if (targetType == LocalDateTime.class) {
                return value instanceof LocalDateTime ? 
                    value : 
                    LocalDateTime.parse(value.toString());
            }
            else if (targetType == LocalDate.class) {
                return value instanceof LocalDate ? 
                    value : 
                    LocalDate.parse(value.toString());
            }
            else {
                throw new IllegalArgumentException(
                    "Unsupported type conversion: " + targetType.getSimpleName());
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Failed to convert value '" + value + "' to type " + 
                targetType.getSimpleName(), e);
        }
    }
    
    /**
     * Interpolates missing values in specified columns using various methods.
     *
     * @param df the DataFrame to process
     * @param method interpolation method ("linear", "spline", "loess", or "neville")
     * @param columns array of column names to interpolate
     * @return new DataFrame with interpolated values
     * @throws IllegalArgumentException if method is invalid or interpolation fails
     */
    public DataFrame interpolate(DataFrame df, String method, String... columns) {
        List<Map<String, Object>> interpolatedData = new ArrayList<>(df.getData());
        
        for (String column : columns) {
      
            List<WeightedObservedPoint> points = collectPoints(interpolatedData, column);
            
     
            UnivariateInterpolator interpolator = selectInterpolator(method);
            
            try {
            
                UnivariateFunction function = interpolator.interpolate(
                    points.stream().mapToDouble(WeightedObservedPoint::getX).toArray(),
                    points.stream().mapToDouble(WeightedObservedPoint::getY).toArray()
                );
                
           
                applyInterpolation(interpolatedData, column, points, function);
                
            } catch (MathIllegalArgumentException e) {
              
                fallbackInterpolation(interpolatedData, column, points);
            }
        }
        
        return new DataFrame(interpolatedData);
    }

    /**
     * Seleciona o interpolador apropriado baseado no método
     */
    private UnivariateInterpolator selectInterpolator(String method) {
        return switch (method.toLowerCase()) {
            case "linear" -> new LinearInterpolator();
            case "spline" -> new SplineInterpolator();
            case "loess" -> new LoessInterpolator();
            case "neville" -> new NevilleInterpolator();
            default -> throw new IllegalArgumentException(
                "Método de interpolação não suportado: " + method + 
                ". Use 'linear', 'spline', 'loess' ou 'neville'."
            );
        };
    }

    /**
     * Coleta pontos não nulos para interpolação
     */
    private List<WeightedObservedPoint> collectPoints(
            List<Map<String, Object>> data, String column) {
        List<WeightedObservedPoint> points = new ArrayList<>();
        
        for (int i = 0; i < data.size(); i++) {
            Object value = data.get(i).get(column);
            if (value instanceof Number) {
                points.add(new WeightedObservedPoint(
                    1.0,
                    i,   
                    ((Number) value).doubleValue()  
                ));
            }
        }
        
        return points;
    }

   
    private void applyInterpolation(
            List<Map<String, Object>> data,
            String column,
            List<WeightedObservedPoint> points,
            UnivariateFunction function) {
       
        double minX = points.stream()
            .mapToDouble(WeightedObservedPoint::getX)
            .min()
            .orElse(0.0);
        double maxX = points.stream()
            .mapToDouble(WeightedObservedPoint::getX)
            .max()
            .orElse(data.size() - 1.0);

    
        for (int i = 0; i < data.size(); i++) {
            if (!(data.get(i).get(column) instanceof Number)) {
                if (i >= minX && i <= maxX) {
                    try {
                        double interpolated = function.value(i);
                        data.get(i).put(column, interpolated);
                    } catch (MathIllegalArgumentException e) {
                       
                        interpolatePoint(data, i, column, points);
                    }
                }
            }
        }
    }

    
    private void fallbackInterpolation(
            List<Map<String, Object>> data,
            String column,
            List<WeightedObservedPoint> points) {
        
        DescriptiveStatistics stats = new DescriptiveStatistics();
        points.forEach(p -> stats.addValue(p.getY()));
        
        for (int i = 0; i < data.size(); i++) {
            if (!(data.get(i).get(column) instanceof Number)) {
                interpolatePoint(data, i, column, points);
            }
        }
    }

 
    private void interpolatePoint(
            List<Map<String, Object>> data,
            int index,
            String column,
            List<WeightedObservedPoint> points) {
        
       
        WeightedObservedPoint before = points.stream()
            .filter(p -> p.getX() < index)
            .max(Comparator.comparingDouble(WeightedObservedPoint::getX))
            .orElse(null);
            
        WeightedObservedPoint after = points.stream()
            .filter(p -> p.getX() > index)
            .min(Comparator.comparingDouble(WeightedObservedPoint::getX))
            .orElse(null);

        if (before != null && after != null) {
           
            double dx = after.getX() - before.getX();
            double dy = after.getY() - before.getY();
            double ratio = (index - before.getX()) / dx;
            double interpolated = before.getY() + (ratio * dy);
            data.get(index).put(column, interpolated);
        } else if (before != null) {
            data.get(index).put(column, before.getY());
        } else if (after != null) {
            data.get(index).put(column, after.getY());
        }
    }

    
    
}