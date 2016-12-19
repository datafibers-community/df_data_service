package com.datafibers.util;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

import java.util.ArrayList;
import java.util.List;

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
}
