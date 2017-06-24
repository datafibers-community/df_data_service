package com.datafibers.util;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

/**
 * Created by DUW3 on 11/17/2016.
 */
public class MongoAdminClient {
    private MongoClient mongoClient;
    private MongoDatabase database;


    public MongoAdminClient(String hostname, int port, String database) {
        this.mongoClient = new MongoClient(hostname, port );
        this.database = this.mongoClient.getDatabase(database);

    }

    public MongoAdminClient dropCollection(String colName) {
        this.database.getCollection(colName).drop();
        return this;
    }

    public MongoAdminClient createCollection(String colName) {
        if(!collectionExists(colName))
        this.database.createCollection(colName);
        return this;
    }

    public boolean collectionExists(String collectionName) {
        if (this.database == null) {
            return false;
        }

        final MongoIterable<String> iterable = database.listCollectionNames();
        try (final MongoCursor<String> it = iterable.iterator()) {
            while (it.hasNext()) {
                if (it.next().equalsIgnoreCase(collectionName)) {
                    return true;
                }
            }
        }

        return false;
    }
}
