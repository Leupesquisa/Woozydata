package com.leumanuel.woozydata.service;

import com.leumanuel.woozydata.model.DataFrame;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.math3.linear.BlockRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

/**
 * Service class for merging and concatenating DataFrames.
 * Provides various methods for combining data from multiple DataFrames.
 *
 * @author Leu A. Manuel
 * @version 1.0
 */
public class MergeService {
    
    /**
     * Merges two DataFrames based on matching columns.
     *
     * @param df1 First DataFrame
     * @param df2 Second DataFrame
     * @param how Type of merge ("inner", "left", "right", "outer")
     * @param on Columns to merge on
     * @return Merged DataFrame
     * @throws IllegalArgumentException if merge type is not supported
     */
    public DataFrame merge(DataFrame df1, DataFrame df2, String how, String... on) {
        switch (how.toLowerCase()) {
            case "inner" -> {
                return performInnerMerge(df1, df2, on);
            }
            case "left" -> {
                return performLeftMerge(df1, df2, on);
            }
            case "right" -> {
                return performRightMerge(df1, df2, on);
            }
            case "outer" -> {
                return performOuterMerge(df1, df2, on);
            }
            default -> throw new IllegalArgumentException("Merge type not supported: " + how);
        }
    }

    /**
     * Concatenates two DataFrames either vertically or horizontally.
     *
     * @param df1 First DataFrame
     * @param df2 Second DataFrame
     * @param axis true for vertical concatenation, false for horizontal
     * @return Concatenated DataFrame
     */
    public DataFrame concat(DataFrame df1, DataFrame df2, boolean axis) {
        if (axis) {
            // Concatenação vertical (empilhar)
            List<Map<String, Object>> concatenatedData = new ArrayList<>();
            concatenatedData.addAll(df1.getData());
            concatenatedData.addAll(df2.getData());
            return new DataFrame(concatenatedData);
        } else {
            // Concatenação horizontal (juntar colunas)
            return horizontalConcat(df1, df2);
        }
    }

    /**
     * Reshapes DataFrame into specified dimensions.
     *
     * @param df DataFrame to reshape
     * @param rows Number of rows in new shape
     * @param cols Number of columns in new shape
     * @return Reshaped DataFrame
     * @throws IllegalArgumentException if dimensions mismatch
     */
    public DataFrame reshape(DataFrame df, int rows, int cols) {
        // Usar BlockRealMatrix do Commons Math para manipulação matricial
        List<Object> flattenedData = df.getData().stream()
            .flatMap(row -> row.values().stream())
            .collect(Collectors.toList());

        if (flattenedData.size() != rows * cols) {
            throw new IllegalArgumentException("Cannot reshape: dimensions mismatch");
        }

        BlockRealMatrix matrix = new BlockRealMatrix(rows, cols);
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                Object value = flattenedData.get(i * cols + j);
                matrix.setEntry(i, j, value instanceof Number ? 
                    ((Number) value).doubleValue() : Double.NaN);
            }
        }

        return createDataFrameFromMatrix(matrix);
    }

    private DataFrame performInnerMerge(DataFrame df1, DataFrame df2, String... on) {
        Map<List<Object>, List<Map<String, Object>>> leftMap = createKeyMap(df1, on);
        Map<List<Object>, List<Map<String, Object>>> rightMap = createKeyMap(df2, on);

        List<Map<String, Object>> mergedData = new ArrayList<>();
        leftMap.forEach((key, leftRows) -> {
            if (rightMap.containsKey(key)) {
                List<Map<String, Object>> rightRows = rightMap.get(key);
                createMergedRows(leftRows, rightRows, mergedData);
            }
        });

        return new DataFrame(mergedData);
    }

    private DataFrame performLeftMerge(DataFrame df1, DataFrame df2, String... on) {
        Map<List<Object>, List<Map<String, Object>>> rightMap = createKeyMap(df2, on);
        List<Map<String, Object>> mergedData = new ArrayList<>();

        df1.getData().forEach(leftRow -> {
            List<Object> key = Arrays.stream(on)
                .map(leftRow::get)
                .collect(Collectors.toList());

            if (rightMap.containsKey(key)) {
                List<Map<String, Object>> rightRows = rightMap.get(key);
                rightRows.forEach(rightRow -> {
                    Map<String, Object> mergedRow = new HashMap<>(leftRow);
                    mergedRow.putAll(rightRow);
                    mergedData.add(mergedRow);
                });
            } else {
                mergedData.add(new HashMap<>(leftRow));
            }
        });

        return new DataFrame(mergedData);
    }

    private DataFrame performRightMerge(DataFrame df1, DataFrame df2, String... on) {
        return performLeftMerge(df2, df1, on);
    }

    private DataFrame performOuterMerge(DataFrame df1, DataFrame df2, String... on) {
        DataFrame leftMerge = performLeftMerge(df1, df2, on);
        DataFrame rightMerge = performLeftMerge(df2, df1, on);

        return concat(leftMerge, rightMerge, true);
    }

    private Map<List<Object>, List<Map<String, Object>>> createKeyMap(
            DataFrame df, String... on) {
        return df.getData().stream()
            .collect(Collectors.groupingBy(
                row -> Arrays.stream(on)
                    .map(row::get)
                    .collect(Collectors.toList())
            ));
    }

    private void createMergedRows(List<Map<String, Object>> leftRows,
                                List<Map<String, Object>> rightRows,
                                List<Map<String, Object>> mergedData) {
        leftRows.forEach(leftRow -> 
            rightRows.forEach(rightRow -> {
                Map<String, Object> mergedRow = new HashMap<>(leftRow);
                mergedRow.putAll(rightRow);
                mergedData.add(mergedRow);
            })
        );
    }

    private DataFrame horizontalConcat(DataFrame df1, DataFrame df2) {
        List<Map<String, Object>> concatenatedData = new ArrayList<>();
        int maxRows = Math.max(df1.getData().size(), df2.getData().size());

        for (int i = 0; i < maxRows; i++) {
            Map<String, Object> newRow = new HashMap<>();
            if (i < df1.getData().size()) {
                newRow.putAll(df1.getData().get(i));
            }
            if (i < df2.getData().size()) {
                newRow.putAll(df2.getData().get(i));
            }
            concatenatedData.add(newRow);
        }

        return new DataFrame(concatenatedData);
    }

    private DataFrame createDataFrameFromMatrix(RealMatrix matrix) {
        List<Map<String, Object>> data = new ArrayList<>();
        for (int i = 0; i < matrix.getRowDimension(); i++) {
            Map<String, Object> row = new HashMap<>();
            for (int j = 0; j < matrix.getColumnDimension(); j++) {
                row.put("col_" + j, matrix.getEntry(i, j));
            }
            data.add(row);
        }
        return new DataFrame(data);
    }
}