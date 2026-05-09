package com.github.swim_developer.ed254.consumer.infrastructure.out.persistence;

import com.github.swim_developer.framework.persistence.mongodb.AbstractMongoIndexInitializer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Slf4j
@ApplicationScoped
public class Ed254MongoIndexInitializer extends AbstractMongoIndexInitializer {

    private static final String FIELD_AERODROME = "aerodromeDesignator";
    private static final String FIELD_RECEIVED_AT = "receivedAt";

    @ConfigProperty(name = "quarkus.mongodb.database")
    String databaseName;

    @ConfigProperty(name = "swim.ed254-event.ttl-days", defaultValue = "90")
    int arrivalEventTtlDays;

    protected Ed254MongoIndexInitializer() {
        this(null);
    }

    @Inject
    public Ed254MongoIndexInitializer(MongoClient mongoClient) {
        super(mongoClient);
    }

    public void onStart(@Observes StartupEvent event) {
        super.onStart();
    }

    @Override
    protected String getDatabaseName() {
        return databaseName;
    }

    @Override
    protected void defineIndexes(MongoDatabase database) {
        defineArrivalEventIndexes(database);
        defineDlqIndexes(database);
        log.info("ED-254 indexes configured (ArrivalEvent TTL: {} days)", arrivalEventTtlDays);
    }

    private void defineArrivalEventIndexes(MongoDatabase database) {
        MongoCollection<Document> c = database.getCollection("ed254_arrival_events");

        createIndex(c, "aerodromeDesignator_1", Indexes.ascending(FIELD_AERODROME), null);
        createIndex(c, "messageType_1", Indexes.ascending("messageType"), null);
        createIndex(c, "receivedAt_-1", Indexes.descending(FIELD_RECEIVED_AT), null);
        createIndex(c, "subscriptionId_1_contentHash_1",
                Indexes.compoundIndex(Indexes.ascending("subscriptionId"), Indexes.ascending("contentHash")), null);
        createIndex(c, "kafkaStatus_1", Indexes.ascending("kafkaStatus"), null);
        createIndex(c, "aerodromeDesignator_1_messageType_1",
                Indexes.compoundIndex(Indexes.ascending(FIELD_AERODROME), Indexes.ascending("messageType")), null);
        createIndex(c, "aerodromeDesignator_1_receivedAt_-1",
                Indexes.compoundIndex(Indexes.ascending(FIELD_AERODROME), Indexes.descending(FIELD_RECEIVED_AT)), null);
        createIndex(c, "receivedAt_ttl", Indexes.ascending(FIELD_RECEIVED_AT), ttlOptions(arrivalEventTtlDays));
    }

    private void defineDlqIndexes(MongoDatabase database) {
        MongoCollection<Document> c = database.getCollection("ed254_dead_letter_queue");

        createIndex(c, "failedAt_-1", Indexes.descending("failedAt"), null);
        createIndex(c, "errorType_1", Indexes.ascending("errorType"), null);
    }
}
