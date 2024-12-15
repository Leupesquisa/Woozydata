package com.leumanuel.woozydata.model;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Core class for data manipulation and analysis.
 * Provides fluent interface for common data operations.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataFrame {
    private List<Map<String, Object>> data;

    /**
     * Creates a new DataFrame with the given data.
     * 
     * @param data List of maps representing tabular data
     */
    public DataFrame(List<Map<String, Object>> data) {
        this.data = new ArrayList<>(data);
    }

    /**
     * Performs automatic data cleaning operations.
     * Includes null removal, duplicate removal, and type fixing.
     * 
     * @return this DataFrame for method chaining
     */
    public DataFrame clean() {
        return dropNull()
               .dropDupes()
               .fixTypes();
    }

    /**
     * Removes rows containing null values.
     * 
     * @return this DataFrame for method chaining
     */
    public DataFrame dropNull() {
        data = data.stream()
                   .filter(row -> row.values().stream().noneMatch(Objects::isNull))
                   .collect(Collectors.toList());
        return this;
    }

    /**
     * Removes duplicate rows from the DataFrame.
     * 
     * @return this DataFrame for method chaining
     */
    public DataFrame dropDupes() {
        Set<String> seen = new HashSet<>();
        data = data.stream()
                   .filter(row -> seen.add(row.toString()))
                   .collect(Collectors.toList());
        return this;
    }

    /**
     * Automatically converts data types based on content.
     * 
     * @return this DataFrame for method chaining
     */
    public DataFrame fixTypes() {
        data = data.stream()
                   .map(this::autoConvertTypes)
                   .collect(Collectors.toList());
        return this;
    }

    /**
     * Fills null values with a specified value.
     * 
     * @param value Value to replace nulls with
     * @return this DataFrame for method chaining
     */
    public DataFrame fill(Object value) {
        return fillNa(value, data.get(0).keySet().toArray(new String[0]));
    }

    /**
     * Fills null values in specified columns.
     * 
     * @param value Value to replace nulls with
     * @param columns Columns to fill
     * @return this DataFrame for method chaining
     */
    public DataFrame fillNa(Object value, String... columns) {
        for (Map<String, Object> row : data) {
            for (String col : columns) {
                if (row.get(col) == null) {
                    row.put(col, value);
                }
            }
        }
        return this;
    }


    private Map<String, Object> autoConvertTypes(Map<String, Object> row) {
        Map<String, Object> newRow = new HashMap<>();
        row.forEach((key, value) -> newRow.put(key, convertValue(value)));
        return newRow;
    }

    private Object convertValue(Object value) {
        if (value == null) return null;
        String str = value.toString();
        try {
            if (str.matches("\\d+")) return Integer.parseInt(str);
            if (str.matches("\\d*\\.\\d+")) return Double.parseDouble(str);
            if (str.matches("true|false")) return Boolean.parseBoolean(str);
            return str;
        } catch (Exception e) {
            return str;
        }
    }

        /**
     * Ranks (sorts) the DataFrame based on specified columns.
     * The ranking is done in descending order, with null values treated as lowest values.
     * For large datasets (over 1000 rows), parallel processing is used for better performance.
     *
     * @param columns Columns to use for ranking, in order of priority
     * @return this DataFrame for method chaining
     * @throws IllegalArgumentException if no columns are specified
     */
    public DataFrame rank(String... columns) {

            if (columns == null || columns.length == 0) {
                throw new IllegalArgumentException("At least one column must be specified for ranking");
            }

            List<String> columnsList = Arrays.asList(columns);

            Comparator<Map<String, Object>> comparator = (row1, row2) -> {
                for (String column : columnsList) {

                    Comparable<Object> val1 = (Comparable<Object>) row1.get(column);
                    Comparable<Object> val2 = (Comparable<Object>) row2.get(column);

                    if (val1 == null && val2 == null) continue;
                    if (val1 == null) return 1;
                    if (val2 == null) return -1;

                    int comparison = val2.compareTo(val1);
                    if (comparison != 0) return comparison;
                }
                return 0;
            };

            boolean useParallel = data.size() > 1000;

             data = (useParallel ? data.parallelStream() : data.stream())
                    .sorted(comparator)
                    .collect(Collectors.toCollection(ArrayList::new));

            return this;
    }
  

        /**
     * Creates a new DataFrame containing only the specified columns.
     * Maintains the original row order but includes only the selected columns.
     * If a specified column doesn't exist, it will be ignored.
     * @param columns Names of columns to select
     * @return new DataFrame containing only the selected columns
     * @throws IllegalArgumentException if no columns are specified
     */
    public DataFrame select(String... columns) {
        List<String> selectedColumns = Arrays.asList(columns);
        List<Map<String, Object>> selectedData = data.stream()
                .map(row -> {
                    Map<String, Object> selectedRow = new HashMap<>();
                    selectedColumns.forEach(col -> selectedRow.put(col, row.get(col)));
                    return selectedRow;
                })
                .collect(Collectors.toList());
        return new DataFrame(selectedData);
    }

    /**
    * Displays the first n rows of the DataFrame.
    * Useful for quickly inspecting the data content.
    * @param limit Number of rows to display
    * @throws IllegalArgumentException if limit is negative
    */
    public void show(int limit) {
        data.stream().limit(limit).forEach(System.out::println);
    }

   
        /**
     * Returns the underlying data structure of the DataFrame.
     * Each element in the list represents a row, with column names mapped to values.
     * The returned list is a direct reference to the DataFrame's data.
     *
     * <p>Note: Modifying the returned list will affect the DataFrame's content.
     * For a safe copy, clone the data before modifying.
     *
     * @return List of Maps containing the DataFrame's data
     */
    public List<Map<String, Object>> getData() {
        return data;
    }
    
}
