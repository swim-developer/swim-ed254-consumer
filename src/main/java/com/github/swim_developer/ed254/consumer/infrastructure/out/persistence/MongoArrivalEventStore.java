package com.github.swim_developer.ed254.consumer.infrastructure.out.persistence;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.port.out.ArrivalEventStore;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document.ArrivalEventDocument;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.consumer.application.port.out.SwimEventCountPort;
import com.github.swim_developer.framework.consumer.application.port.out.SwimOutboxEventStorePort;
import com.github.swim_developer.framework.consumer.infrastructure.out.idempotency.SwimIdempotencyEventPort;
import com.github.swim_developer.framework.domain.model.SwimOutboxEvent;
import io.quarkus.mongodb.panache.PanacheMongoRepository;
import jakarta.enterprise.context.ApplicationScoped;
import org.bson.types.ObjectId;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class MongoArrivalEventStore implements PanacheMongoRepository<ArrivalEventDocument>, ArrivalEventStore, SwimIdempotencyEventPort, SwimOutboxEventStorePort, SwimEventCountPort {

    public ArrivalEventDocument findEventDocumentById(String id) {
        return findById(new ObjectId(id));
    }

    @Override
    public List<ArrivalEvent> listAllDomain() {
        return listAll().stream().map(ArrivalEventDocument::toDomain).toList();
    }

    @Override
    public Optional<ArrivalEvent> findById(String id) {
        ArrivalEventDocument doc = findById(new ObjectId(id));
        return Optional.ofNullable(doc).map(ArrivalEventDocument::toDomain);
    }

    @Override
    public Optional<ArrivalEvent> findByMessageId(String messageId) {
        return find("messageId", messageId).firstResultOptional().map(ArrivalEventDocument::toDomain);
    }

    @Override
    public long countByAerodrome(String aerodrome) {
        return count("aerodromeDesignator", aerodrome);
    }

    @Override
    public boolean existsByContentHash(String contentHash) {
        return count("contentHash", contentHash) > 0;
    }

    @Override
    public boolean existsBySubscriptionAndContentHash(String subscriptionId, String contentHash) {
        return count("subscriptionId = ?1 and contentHash = ?2", subscriptionId, contentHash) > 0;
    }

    @Override
    public List<ArrivalEvent> findByReceivedAtBetween(Instant start, Instant end, String aerodrome, int page, int size) {
        if (aerodrome != null && !aerodrome.isBlank()) {
            return find("aerodromeDesignator = ?1 and receivedAt >= ?2 and receivedAt <= ?3", aerodrome, start, end)
                    .page(page, size).list().stream().map(ArrivalEventDocument::toDomain).toList();
        }
        return find("receivedAt >= ?1 and receivedAt <= ?2", start, end)
                .page(page, size).list().stream().map(ArrivalEventDocument::toDomain).toList();
    }

    @Override
    public long countByReceivedAtBetween(Instant start, Instant end, String aerodrome) {
        if (aerodrome != null && !aerodrome.isBlank()) {
            return count("aerodromeDesignator = ?1 and receivedAt >= ?2 and receivedAt <= ?3", aerodrome, start, end);
        }
        return count("receivedAt >= ?1 and receivedAt <= ?2", start, end);
    }

    @Override
    public List<String> findRecentContentHashes(Instant since, int limit) {
        return find("receivedAt >= ?1", since)
                .page(0, limit)
                .project(ArrivalEventHashProjection.class)
                .list().stream()
                .map(ArrivalEventHashProjection::getContentHash)
                .filter(java.util.Objects::nonNull)
                .toList();
    }

    @Override
    public List<String> findRecentCacheKeys(Instant since, int limit) {
        return find("receivedAt >= ?1", since)
                .page(0, limit)
                .project(ArrivalEventHashProjection.class)
                .list().stream()
                .filter(p -> p.getSubscriptionId() != null && p.getContentHash() != null)
                .map(p -> p.getSubscriptionId() + ":" + p.getContentHash())
                .toList();
    }

    @Override
    public void persist(ArrivalEvent event) {
        PanacheMongoRepository.super.persist(ArrivalEventDocument.fromDomain(event));
    }

    @Override
    public void persist(List<ArrivalEvent> events) {
        List<ArrivalEventDocument> docs = events.stream().map(ArrivalEventDocument::fromDomain).toList();
        PanacheMongoRepository.super.persist(docs);
    }

    @Override
    public void update(ArrivalEvent event) {
        ArrivalEventDocument doc = ArrivalEventDocument.fromDomain(event);
        PanacheMongoRepository.super.update(doc);
    }

    @Override
    public long count() {
        return mongoCollection().countDocuments();
    }

    @Override
    public long countEvents() {
        return count();
    }

    @Override
    public List<? extends SwimOutboxEvent> findPendingOutboxEvents(int batchSize) {
        return find("kafkaStatus", OutboxDeliveryStatus.PENDING)
                .page(0, batchSize).list().stream()
                .map(ArrivalEventDocument::toDomain).toList();
    }

    @Override
    public void updateOutboxEvent(SwimOutboxEvent event) {
        ArrivalEventDocument doc = ArrivalEventDocument.fromDomain((ArrivalEvent) event);
        PanacheMongoRepository.super.update(doc);
    }
}
