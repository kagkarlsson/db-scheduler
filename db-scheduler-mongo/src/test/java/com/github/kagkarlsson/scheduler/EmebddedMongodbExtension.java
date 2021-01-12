package com.github.kagkarlsson.scheduler;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import com.github.kagkarlsson.scheduler.utils.TestUtils;
import com.github.kagkarlsson.scheduler.utils.TestUtils.MongoTools;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmebddedMongodbExtension implements AfterEachCallback, AfterAllCallback {

    private static final Logger LOG = LoggerFactory.getLogger(EmebddedMongodbExtension.class);

    private MongoTools mongoTools;
    private MongoCollection<TaskEntity> collection;
    private MongoDatabase db;

    public EmebddedMongodbExtension() {
        this("db-scheduler", "db-scheduler");
    }

    public EmebddedMongodbExtension(String collectionName, String databaseName) {
        try {
            mongoTools = TestUtils.startEmbeddedMongo();
        } catch (IOException e) {
            e.printStackTrace();
        }
        db = this.getDatabase(databaseName);
        collection = this.getCollection(db, collectionName);
    }

    private MongoDatabase getDatabase(String databaseName) {
        CodecRegistry pojoCodecRegistry = fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        return mongoTools.getClient().getDatabase(databaseName)
            .withCodecRegistry(pojoCodecRegistry);
    }

    private MongoCollection<TaskEntity> getCollection(MongoDatabase db,
        String collectionName) {
        MongoCollection<TaskEntity> collection = db
            .getCollection(collectionName, TaskEntity.class);
        return collection;
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        LOG.info("Delete collection");
        this.collection.deleteMany(new Document());
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws Exception {
        LOG.info("Kill embedded mongo");
        this.mongoTools.getMongodProcess().stop();
    }

    public MongoCollection<TaskEntity> getCollection() {
        return collection;
    }

    public MongoDatabase getDb() {
        return db;
    }

    public MongoClient getMongoClient() {
        return this.mongoTools.getClient();
    }
}
