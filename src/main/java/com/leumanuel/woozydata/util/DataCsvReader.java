package com.leumanuel.woozydata.util;

/***
 * @author Leu A.Manuel
 */

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import com.leumanuel.woozydata.model.DataFrame;

import java.io.FileReader;
import java.io.Reader;
import java.util.*;


/**
 * Utility class for reading CSV files into DataFrame objects.
 * Uses Apache Commons CSV for parsing CSV files with headers.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataCsvReader {
    
    /**
     * Reads a CSV file and converts it to a DataFrame.
     * The first row of the CSV file is expected to contain headers.
     * 
     * @param filePath Path to the CSV file to read
     * @return DataFrame containing the CSV data
     * @throws Exception if there is an error reading or parsing the file
     */
     public static DataFrame readCSV(String filePath) throws Exception {
        List<Map<String, Object>> data = new ArrayList<>();
        try (Reader in = new FileReader(filePath)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            for (CSVRecord record : records) {
                Map<String, Object> row = new HashMap<>();
                record.toMap().forEach(row::put);
                data.add(row);
            }
        }
        return new DataFrame(data);
    }
}
