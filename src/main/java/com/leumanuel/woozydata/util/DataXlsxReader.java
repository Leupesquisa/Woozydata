
package com.leumanuel.woozydata.util;


import com.leumanuel.woozydata.model.DataFrame;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.util.*;


/**
 * Utility class for reading Excel (XLSX) files into DataFrame objects.
 * Uses Apache POI for Excel file parsing.
 * 
 * @author Leu A. Manuel
 * @version 1.0
 */
public class DataXlsxReader {
    
    /**
     * Reads an Excel file and converts it to a DataFrame.
     * Assumes first row contains headers and reads from first sheet.
     * Supports string, numeric, and boolean cell types.
     * 
     * @param filePath Path to the Excel file to read
     * @return DataFrame containing the Excel data
     * @throws Exception if there is an error reading or parsing the file
     */
    public static DataFrame readXLSX(String filePath) throws Exception {
        List<Map<String, Object>> data = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(filePath);
             Workbook workbook = new XSSFWorkbook(fis)) {
            Sheet sheet = workbook.getSheetAt(0);
            List<String> headers = new ArrayList<>();
            for (Cell cell : sheet.getRow(0)) {
                headers.add(cell.getStringCellValue());
            }

            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Map<String, Object> rowData = new HashMap<>();
                Row row = sheet.getRow(i);
                for (int j = 0; j < headers.size(); j++) {
                    Cell cell = row.getCell(j);
                    Object value = switch (cell.getCellType()) {
                        case STRING -> cell.getStringCellValue();
                        case NUMERIC -> cell.getNumericCellValue();
                        case BOOLEAN -> cell.getBooleanCellValue();
                        default -> null;
                    };
                    rowData.put(headers.get(j), value);
                }
                data.add(rowData);
            }
        }
        return new DataFrame(data);
    }
}

