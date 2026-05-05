package com.github.swim_developer.ed254.consumer.domain.port.out;

import com.github.swim_developer.ed254.consumer.domain.model.Subscription;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface SubscriptionStore {

    void persistSubscription(Subscription subscription);

    Optional<Subscription> findBySubscriptionId(String subscriptionId);

    Optional<Subscription> findByConfigHash(String configHash);

    Optional<Subscription> findByConfigHashAndType(String configHash, String type);

    Optional<Subscription> findByQueueName(String queueName);

    List<Subscription> findAllSubscriptions();

    List<Subscription> findActiveSubscriptions();

    List<Subscription> findDeclaredSubscriptions();

    List<Subscription> findBySubscriptionEndBefore(Instant threshold);

    void updateSubscription(Subscription subscription);

    void updateStatus(String subscriptionId, String status);

    boolean deleteBySubscriptionId(String subscriptionId);

    void deleteAllSubscriptions();

    long countSubscriptions();
}
