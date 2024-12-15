
package com.leumanuel.woozydata.config;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/**
 * Configuration class for MongoDB connection management.
 * Handles the creation and management of MongoDB client connections.
 *
 * @author Leu A. Manuel
 * @version 1.0
 */
public class MongoConfig {
    private final String connectionString;
    private final String dbName;
    private MongoClient mongoClient;

   
    /**
     * Creates a new MongoDB configuration.
     *
     * @param connectionString MongoDB connection string
     * @param dbName Database name to connect to
     */
    public MongoConfig(String connectionString, String dbName) {
        this.connectionString = connectionString;
        this.dbName = dbName;
        this.mongoClient = MongoClients.create(connectionString);
    }

     /**
     * Gets the configured MongoDB database instance.
     *
     * @return MongoDatabase instance
     */
    public MongoDatabase getDatabase() {
        return mongoClient.getDatabase(dbName);
    }

    /**
     * Closes the MongoDB client connection.
     * Should be called when the connection is no longer needed.
     */
    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
