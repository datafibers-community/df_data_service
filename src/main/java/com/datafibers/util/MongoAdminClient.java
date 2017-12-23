package com.datafibers.util;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import io.vertx.core.json.JsonObject;
import org.bson.Document;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
        this.database.getCollection(colName).deleteMany(exists("_id")); //TODO consider to add date filter
        return this;
    }

    public MongoAdminClient truncateCollectionExcept(String colName, String key, String value) {
        if(collectionExists(colName))
            this.database.getCollection(colName).deleteMany(ne(key, value));
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
        if(collectionExists(colName))
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

    /**
     * Lookup A collection using specific key and value and return value from another value
     * @param lkpKey
     * @param lkpValue
     * @param lkpReturnedKey
     * @return lkpReturnedKValue
     */
    public String lkpCollection(String lkpKey, String lkpValue, String lkpReturnedKey) {
        String connectorClass = this.collection.find(eq(lkpKey, lkpValue)).first().toJson();
        return new JsonObject(connectorClass).getString(lkpReturnedKey);
    }

    public MongoAdminClient importJsonFile(String fileNamePath) {
        int count = 0;
        int batch = 100;

        List<InsertOneModel<Document>> docs = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileNamePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                docs.add(new InsertOneModel<>(Document.parse(line)));
                count++;
                if (count == batch) {
                    this.collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
                    docs.clear();
                    count = 0;
                }
            }
        } catch (IOException fnfe) {
            fnfe.printStackTrace();
        }

        if (count > 0) {
            collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
        }

        return this;
    }

    public MongoAdminClient importJsonInputStream(InputStream fileInputStream) {
        int count = 0;
        int batch = 100;

        List<InsertOneModel<Document>> docs = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream))) {
            String line;
            while ((line = br.readLine()) != null) {
                docs.add(new InsertOneModel<>(Document.parse(line)));
                count++;
                if (count == batch) {
                    this.collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
                    docs.clear();
                    count = 0;
                }
            }
        } catch (IOException fnfe) {
            fnfe.printStackTrace();
        }

        if (count > 0) {
            collection.bulkWrite(docs, new BulkWriteOptions().ordered(false));
        }

        return this;
    }

    public void close() {
        this.mongoClient.close();
    }
}
