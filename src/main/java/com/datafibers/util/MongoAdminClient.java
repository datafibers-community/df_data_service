package com.datafibers.util;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import static com.mongodb.client.model.Filters.eq;

import io.vertx.core.json.JsonObject;
import org.bson.Document;

/**
 * MongoDB Client
 */
public class MongoAdminClient {
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;


    public MongoAdminClient(String hostname, int port, String database) {
        this.mongoClient = new MongoClient(hostname, port );
        this.database = this.mongoClient.getDatabase(database);
    }

    public MongoAdminClient(String hostname, String port, String database) {
        this.mongoClient = new MongoClient(hostname, Integer.parseInt(port));
        this.database = this.mongoClient.getDatabase(database);
    }

    public MongoAdminClient(String hostname, String port, String database, String collection) {
        this.mongoClient = new MongoClient(hostname, Integer.parseInt(port));
        this.database = this.mongoClient.getDatabase(database);
        this.collection = this.database.getCollection(collection);
    }

    public MongoAdminClient truncateCollection(String colName) {
        if(collectionExists(colName))
        this.database.getCollection(colName).deleteMany(new Document()); //TODO add date filter
        return this;
    }

    public MongoAdminClient dropCollection(String colName) {
        if(collectionExists(colName))
        this.database.getCollection(colName).drop();
        return this;
    }

    public MongoAdminClient createCollection(String colName) {
        if(!collectionExists(colName))
        this.database.createCollection(colName);
        return this;
    }

    public MongoAdminClient useCollection(String colName) {
        if(!collectionExists(colName))
            this.collection = this.database.getCollection(colName);
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

    public String findConnectorClassName(String connectorType) {
        String connectorClass = this.collection.find(eq("connectorType", connectorType)).first().toJson();
        return new JsonObject(connectorClass).getString("class");
    }
}
