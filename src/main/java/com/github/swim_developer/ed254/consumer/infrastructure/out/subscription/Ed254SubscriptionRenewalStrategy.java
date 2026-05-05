package com.github.swim_developer.ed254.consumer.infrastructure.out.subscription;

import com.github.swim_developer.ed254.consumer.infrastructure.out.client.ProviderRestClient;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionResponse;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoSubscriptionStore;
import com.github.swim_developer.framework.domain.model.SubscriptionStatus;
import com.github.swim_developer.framework.domain.model.SubscriptionRenewalInfo;
import com.github.swim_developer.framework.domain.exception.SubscriptionRenewalException;
import com.github.swim_developer.framework.application.port.out.SubscriptionRenewalStrategy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

import com.github.swim_developer.ed254.consumer.application.usecase.Ed254SubscriptionUseCase;
import com.github.swim_developer.ed254.consumer.infrastructure.out.client.Ed254SubscriptionManagerAdapter;

@Slf4j
@ApplicationScoped
public class Ed254SubscriptionRenewalStrategy implements SubscriptionRenewalStrategy {

    private final MongoSubscriptionStore subscriptionRepository;
    private final ProviderRestClient providerClient;

    @Inject
    public Ed254SubscriptionRenewalStrategy(MongoSubscriptionStore subscriptionRepository,
                                            ProviderRestClient providerClient) {
        this.subscriptionRepository = subscriptionRepository;
        this.providerClient = providerClient;
    }

    @Override
    public List<SubscriptionRenewalInfo> findSubscriptionsNearExpiry(Instant threshold) {
        return subscriptionRepository.findBySubscriptionEndBefore(threshold)
                .stream()
                .filter(sub -> SubscriptionStatus.ACTIVE.name().equals(sub.getSubscriptionStatus()))
                .map(sub -> new SubscriptionRenewalInfo(
                        sub.getSubscriptionId(),
                        sub.getSubscriptionEnd()
                ))
                .toList();
    }

    @Override
    public void renewSubscription(String subscriptionId) throws SubscriptionRenewalException {
        log.info("Calling provider to renew subscription: {}", subscriptionId);

        final SubscriptionResponse response;
        try {
            response = providerClient.renewSubscription(subscriptionId);
        } catch (Exception e) {
            throw new SubscriptionRenewalException(subscriptionId, e);
        }

        Subscription subscription = subscriptionRepository.findBySubscriptionId(subscriptionId)
                .orElseThrow(() -> new IllegalStateException("Subscription not found: " + subscriptionId));

        subscription.setSubscriptionEnd(response.subscriptionEnd());
        subscriptionRepository.updateSubscription(subscription);

        log.info("Subscription renewed locally - ID: {}, New subscriptionEnd: {}",
                subscriptionId, response.subscriptionEnd());
    }
}
