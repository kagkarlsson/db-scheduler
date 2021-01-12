package com.github.kagkarlsson.scheduler.utils;

import com.mongodb.MongoClient;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class TestUtils {

    public static class MongoTools {

        MongoClient client;
        MongodExecutable mongodExecutable;
        MongodProcess mongodProcess;

        public MongoClient getClient() {
            return client;
        }

        public void setClient(MongoClient client) {
            this.client = client;
        }

        public MongodExecutable getMongodExecutable() {
            return mongodExecutable;
        }

        public void setMongodExecutable(MongodExecutable mongodExecutable) {
            this.mongodExecutable = mongodExecutable;
        }

        public MongodProcess getMongodProcess() {
            return mongodProcess;
        }

        public void setMongodProcess(MongodProcess mongodProcess) {
            this.mongodProcess = mongodProcess;
        }
    }

    public static MongoTools startEmbeddedMongo() throws IOException {
        MongodStarter starter = MongodStarter.getDefaultInstance();

        int port = Network.getFreeServerPort();
        MongodConfig mongodConfig = MongodConfig.builder()
            .version(Version.Main.PRODUCTION)
            .net(new Net(port, Network.localhostIsIPv6()))
            .build();
        MongodExecutable mongodExecutable = starter.prepare(mongodConfig);
        MongodProcess mongod = mongodExecutable.start();

        MongoClient mongoClient = new MongoClient("localhost", port);

        MongoTools mongoTools = new MongoTools();
        mongoTools.setClient(mongoClient);
        mongoTools.setMongodExecutable(mongodExecutable);
        mongoTools.setMongodProcess(mongod);

        return mongoTools;
    }

    public static Instant truncateInstant(Instant instant) {
        return instant.truncatedTo(ChronoUnit.MILLIS);
    }
}
