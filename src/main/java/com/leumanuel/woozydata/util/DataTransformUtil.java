package com.leumanuel.woozydata.util;

import com.leumanuel.woozydata.model.DataFrame;
import java.util.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility class for data transformation and formatting operations.
 * Provides methods for PowerBI compatibility and metadata generation.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataTransformUtil {
   
    /**
     * Formats DataFrame data for PowerBI compatibility.
     * Handles special data types and formatting requirements.
     * 
     * @param df DataFrame to format
     * @return Formatted DataFrame compatible with PowerBI
     */
    public static DataFrame formatForPowerBI(DataFrame df) {
        List<Map<String, Object>> formattedData = new ArrayList<>();
        
        df.getData().forEach(row -> {
            Map<String, Object> newRow = new HashMap<>();
            row.forEach((key, value) -> {
                newRow.put(key, formatValue(value));
            });
            formattedData.add(newRow);
        });
        
        return new DataFrame(formattedData);
    }

    /**
     * Creates metadata DataFrame describing the structure of input DataFrame.
     * Includes column names, data types, and descriptions.
     * 
     * @param df DataFrame to analyze
     * @return DataFrame containing metadata information
     */
    public static DataFrame createMetadata(DataFrame df) {
        List<Map<String, Object>> metadata = new ArrayList<>();
        
        df.getData().get(0).forEach((column, value) -> {
            Map<String, Object> metaRow = new HashMap<>();
            metaRow.put("Column", column);
            metaRow.put("Type", getDataType(value));
            metaRow.put("Description", generateDescription(column));
            metadata.add(metaRow);
        });
        
        return new DataFrame(metadata);
    }

    /**
     * Formats a value for PowerBI compatibility.
     * Handles special cases like DateTime formatting.
     * 
     * @param value Value to format
     * @return Formatted value
     */
    private static Object formatValue(Object value) {
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        return value;
    }

    /**
     * Determines the data type of a value.
     * Maps Java types to PowerBI-compatible type strings.
     * 
     * @param value Value to analyze
     * @return String representation of the data type
     */
    private static String getDataType(Object value) {
        if (value instanceof Number) return "Numeric";
        if (value instanceof LocalDateTime) return "DateTime";
        if (value instanceof Boolean) return "Boolean";
        return "Text";
    }

    /**
     * Generates a human-readable description for a column.
     * Formats column names by removing underscores and proper casing.
     * 
     * @param column Column name to describe
     * @return Generated description
     */
    private static String generateDescription(String column) {
        return "Data for " + column.replace("_", " ").toLowerCase();
    }
}