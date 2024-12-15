package com.leumanuel.woozydata.repository;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.leumanuel.woozydata.config.MongoConfig;
import com.leumanuel.woozydata.model.DataFrame;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Connector class for MongoDB operations.
 * Provides methods for reading data from MongoDB collections into DataFrames.
 *
 * @author Leu A. Manuel
 * @version 1.0
 */
public class MongoDbConnector {
    private final MongoConfig mongoConfig;

    /**
     * Creates a new MongoDB connector with specified configuration.
     *
     * @param mongoConfig MongoDB configuration
     */
    public MongoDbConnector(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
    }

    /**
     * Reads a MongoDB collection into a DataFrame.
     *
     * @param collectionName Name of the collection to read
     * @return DataFrame containing collection data
     */
    public DataFrame readCollection(String collectionName) {
        MongoDatabase database = mongoConfig.getDatabase();
        MongoCollection<Document> collection = database.getCollection(collectionName);
        FindIterable<Document> documents = collection.find();

        List<Map<String, Object>> data = new ArrayList<>();
        for (Document document : documents) {
            Map<String, Object> row = new HashMap<>(document);
            data.add(row);
        }
        
        return new DataFrame(data);
    }

   /**
     * Closes the MongoDB connection.
     * Should be called when the connector is no longer needed.
     */
    public void close() {
        mongoConfig.close();
    }
}
