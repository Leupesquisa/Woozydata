
package com.leumanuel.woozydata.util;

import com.leumanuel.woozydata.model.DataFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Utility class for exporting DataFrame data to different file formats.
 * Supports CSV and JSON export formats.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataExporter {


    /**
     * Exports DataFrame data to a CSV file.
     * Creates a new file or overwrites existing file with headers and data.
     * 
     * @param dataFrame DataFrame to export
     * @param filePath Path where the CSV file should be saved
     * @throws IOException if there is an error writing the file
     */
    public static void exportToCSV(DataFrame dataFrame, String filePath) throws IOException {
        try (FileWriter out = new FileWriter(filePath);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(dataFrame.getData().get(0).keySet().toArray(new String[0])))) {
            for (Map<String, Object> row : dataFrame.getData()) {
                printer.printRecord(row.values());
            }
        }
    }

    
    /**
     * Exports DataFrame data to a JSON file.
     * Creates a new file or overwrites existing file with pretty-printed JSON.
     * 
     * @param dataFrame DataFrame to export
     * @param filePath Path where the JSON file should be saved
     * @throws IOException if there is an error writing the file
     */
    public static void exportToJSON(DataFrame dataFrame, String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(new FileWriter(filePath), dataFrame.getData());
    }
}
