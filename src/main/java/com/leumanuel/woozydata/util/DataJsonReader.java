
package com.leumanuel.woozydata.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leumanuel.woozydata.model.DataFrame;

import java.io.File;
import java.util.*;

/**
 * Utility class for reading JSON files into DataFrame objects.
 * Uses Jackson ObjectMapper for JSON parsing.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataJsonReader {
    
    /**
     * Reads a JSON file and converts it to a DataFrame.
     * Expects JSON array of objects with consistent structure.
     * 
     * @param filePath Path to the JSON file to read
     * @return DataFrame containing the JSON data
     * @throws Exception if there is an error reading or parsing the file
     */
    public static DataFrame readJSON(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> data = mapper.readValue(new File(filePath), List.class);
        return new DataFrame(data);
    }
    
}
