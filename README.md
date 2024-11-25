# Woozydata

Woozydata is a powerful and easy-to-use data analysis library designed to simplify complex tasks like data cleaning, exporting, statistical analysis, and modeling. Perfect for data analysts and full stack developers, Woozydata provides a unified interface for working with tabular data, offering descriptive statistics, cleaning methods, advanced analysis, and support for multiple export formats.

## 🚀 Purpose of the Library

**Woozydata** was built to:
- Make data analysis accessible and efficient with a simple and intuitive API.
- Unify tasks such as cleaning, exporting, statistics, and forecasting into a single library.
- Empower developers and analysts to build robust analytical solutions in Java.

## 🧰 Key Features

1. **Data Loading**:
   - Supports CSV, Excel, JSON files, and MongoDB databases.
2. **Descriptive Statistics**:
   - Calculates mean, median, standard deviation, variance, covariance, and more.
3. **Data Cleaning**:
   - Removes null values and duplicates, fills missing values, normalizes, and standardizes data.
4. **Data Export**:
   - Save your data in CSV, Excel, JSON, Power BI, HTML, or LaTeX formats.
5. **Advanced Analysis**:
   - Supports linear regression, logistic regression, time series forecasting, and more.
6. **Data Transformation**:
   - Pivot tables, melt, binary columns (dummies), and grouping.
7. **Statistical Distributions**:
   - Generate normal, uniform, binomial, and Poisson distributions.
8. **Outlier Detection and Time Series Analysis**.

---

## 📦 Installation

Add the library to your Maven project:

```xml
<dependency>
    <groupId>com.leumanuel</groupId>
    <artifactId>woozydata</artifactId>
    <version>1.0</version>
</dependency>
```
## 🌟 Examples of Usage

1. Initialization and Data Loading
```xml
Woozydata woozydata = new Woozydata();

// Load a JSON file or fromCsv to Load a CSV file  
DataFrame dataFrame = woozydata.fromJson("data/sample_data.json");
System.out.println(dataFrame);

// Load data from a MongoDB database
DataFrame mongoData = woozydata.fromMongo("mongodb://localhost:27017", "myDatabase", "myCollection");
System.out.println(mongoData);
```
2. Descriptive Statistics
```xml
// Calculate statistics
double mean = woozydata.mean("age");
System.out.println("Mean of 'age' column: " + mean);

double median = woozydata.median("salary");
System.out.println("Median of 'salary' column: " + median);

Map<String, Object> description = woozydata.describe("income");
System.out.println("Full description of 'income' column: " + description);
```

3. Data Cleaning
```xml
// Clean the DataFrame
DataFrame cleanedData = woozydata.clean();
System.out.println("Cleaned DataFrame: " + cleanedData);

// Drop null values
DataFrame withoutNa = woozydata.dropNa();
System.out.println("Without null values: " + withoutNa);

// Fill missing values
DataFrame filledData = woozydata.fillNa(0);
System.out.println("Filled missing values: " + filledData);
```


4. Data Export
```xml
// Export to Excel
woozydata.toExcel("output/cleaned_data.xlsx");
System.out.println("Data exported to Excel.");

// Export to JSON
woozydata.toJson("output/data.json");
System.out.println("Data exported to JSON.");
```

5. Data Transformation
```xml
// Create a pivot table
DataFrame pivoted = woozydata.pivot("category", "year", "sales");
System.out.println("Pivoted DataFrame: " + pivoted);

// Melt the DataFrame
DataFrame melted = woozydata.melt(new String[]{"id"}, new String[]{"sales", "profit"});
System.out.println("Melted DataFrame: " + melted);
```

6. Advanced Analysis
```xml
Copiar código
// Linear Regression
double[] regression = woozydata.linearReg("experience", "salary");
System.out.println("Linear Regression (intercept, slope): " + regression[0] + ", " + regression[1]);

// Time Series Forecasting
DataFrame forecasted = woozydata.forecast("date", "sales", 12);
System.out.println("Forecasted DataFrame: " + forecasted);
```

7. Outlier Detection
```xml
// Detect outliers in a column
DataFrame outliers = woozydata.detectOutliers("sales");
System.out.println("Detected outliers: " + outliers);
```

## 🔧 Requirements

Java 17+ 
Apache Commons Math 3.6.1
MongoDB Driver (opcional)

## 📈 Performance
| Operation | 10K Rows | 100K Rows | 1M Rows |
|---|---|---|---|
| Loading Data | 0.5s | 2.3s | 15.1s |
| Data Cleaning | 0.2s | 1.1s | 8.4s |
| Data Analyis | 0.3s | 1.5s | 9.2s |

## 📊 Integration with Dashboards
You can integrate Woozydata with front-end tools like React or Angular to create interactive dashboards. Additionally, export your data to Power BI or HTML files for visual reporting.

## 📖 Complete Documentation
For more details, refer to the official documentation.

## 🛠️ Support
If you encounter issues or have questions, open an issue on GitHub or contact us via email.
Email: leu.manuel@hotmail.com
LinkedIn: [Leu Manuel](https://www.linkedin.com/in/leu-manuel/)

## 📢 Contribute!
We welcome contributions! Check out the contribution guide.
Apache Commons Math team
Java Community

## 🌍 License
This project is licensed under the MIT License.




