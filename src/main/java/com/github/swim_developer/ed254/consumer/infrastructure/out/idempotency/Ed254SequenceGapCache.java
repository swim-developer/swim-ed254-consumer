package com.github.swim_developer.ed254.consumer.infrastructure.out.idempotency;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.swim_developer.ed254.consumer.application.port.out.Ed254SequenceGapCachePort;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document.LastFiledRecordStateDocument;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoLastFiledRecordStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@ApplicationScoped
public class Ed254SequenceGapCache implements Ed254SequenceGapCachePort {

    private static final long MAX_SIZE = 1000;

    private final Cache<String, List<String>> cache = Caffeine.newBuilder()
            .maximumSize(MAX_SIZE)
            .recordStats()
            .build();

    private final Set<String> dirtyQueues = ConcurrentHashMap.newKeySet();

    private final MongoLastFiledRecordStore repository;
    private final MeterRegistry meterRegistry;

    @Inject
    public Ed254SequenceGapCache(MongoLastFiledRecordStore repository, MeterRegistry meterRegistry) {
        this.repository = repository;
        this.meterRegistry = meterRegistry;
    }

    @PostConstruct
    void registerMetrics() {
        CaffeineCacheMetrics.monitor(meterRegistry, cache, "sequence_gap_cache");
    }

    public List<String> getArcids(String queueName) {
        List<String> cached = cache.getIfPresent(queueName);
        if (cached != null) {
            return cached;
        }
        return repository.findByQueueName(queueName)
                .map(state -> {
                    List<String> arcids = state.getArcids() != null ? state.getArcids() : Collections.emptyList();
                    cache.put(queueName, arcids);
                    return arcids;
                })
                .orElse(null);
    }

    public void updateArcids(String queueName, List<String> arcids) {
        cache.put(queueName, List.copyOf(arcids));
        dirtyQueues.add(queueName);
    }

    @Scheduled(every = "5s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void flushDirtyEntries() {
        if (dirtyQueues.isEmpty()) {
            return;
        }
        Set<String> toFlush = Set.copyOf(dirtyQueues);
        dirtyQueues.removeAll(toFlush);

        for (String queueName : toFlush) {
            List<String> arcids = cache.getIfPresent(queueName);
            if (arcids == null) {
                continue;
            }
            try {
                repository.findByQueueName(queueName).ifPresentOrElse(
                        state -> {
                            state.setArcids(arcids);
                            state.setUpdatedAt(Instant.now());
                            repository.update(state);
                        },
                        () -> {
                            LastFiledRecordStateDocument state = new LastFiledRecordStateDocument();
                            state.setQueueName(queueName);
                            state.setArcids(arcids);
                            state.setUpdatedAt(Instant.now());
                            repository.persist(state);
                        }
                );
            } catch (Exception e) {
                log.warn("Failed to flush gap state for queue {}: {}", queueName, e.getMessage());
                dirtyQueues.add(queueName);
            }
        }
        log.debug("Sequence gap state flushed - {} queues", toFlush.size());
    }

    public void removeSubscription(String queueName) {
        repository.deleteByQueueName(queueName);
        cache.invalidate(queueName);
        dirtyQueues.remove(queueName);
        log.info("Sequence gap state removed for queue: {}", queueName);
    }

    public void loadAll() {
        cache.invalidateAll();
        dirtyQueues.clear();
        repository.listAll().forEach(state ->
                cache.put(state.getQueueName(),
                        state.getArcids() != null ? state.getArcids() : Collections.emptyList()));
        log.info("Sequence gap cache loaded - {} subscriptions", cache.estimatedSize());
    }

    public int size() {
        return (int) cache.estimatedSize();
    }
}
