package com.github.swim_developer.ed254.consumer.domain.port.out;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface ArrivalEventStore {

    void persist(ArrivalEvent event);

    void persist(List<ArrivalEvent> events);

    void update(ArrivalEvent event);

    List<ArrivalEvent> listAllDomain();

    Optional<ArrivalEvent> findById(String id);

    Optional<ArrivalEvent> findByMessageId(String messageId);

    long countByAerodrome(String aerodrome);

    boolean existsByContentHash(String contentHash);

    boolean existsBySubscriptionAndContentHash(String subscriptionId, String contentHash);

    List<ArrivalEvent> findByReceivedAtBetween(Instant start, Instant end, String aerodrome, int page, int size);

    long countByReceivedAtBetween(Instant start, Instant end, String aerodrome);

    List<String> findRecentContentHashes(Instant since, int limit);

    List<String> findRecentCacheKeys(Instant since, int limit);

    long count();
}
