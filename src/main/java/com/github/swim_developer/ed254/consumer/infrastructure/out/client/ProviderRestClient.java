package com.github.swim_developer.ed254.consumer.infrastructure.out.client;

import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionResponse;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoSubscriptionStore;
import com.github.swim_developer.framework.consumer.infrastructure.out.config.provider.ProviderConfigParser;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class ProviderRestClient {

    private final Ed254SubscriptionManagerAdapter smClientRegistry;
    private final ProviderConfigParser providerConfigParser;
    private final MongoSubscriptionStore subscriptionRepository;

    @Inject
    public ProviderRestClient(Ed254SubscriptionManagerAdapter smClientRegistry,
                              ProviderConfigParser providerConfigParser,
                              MongoSubscriptionStore subscriptionRepository) {
        this.smClientRegistry = smClientRegistry;
        this.providerConfigParser = providerConfigParser;
        this.subscriptionRepository = subscriptionRepository;
    }

    public SubscriptionResponse renewSubscription(String subscriptionId) throws Exception {
        log.debug("Calling provider to renew subscription: {}", subscriptionId);
        SubscriptionManagerRestClient client = resolveSmClient(subscriptionId);
        return client.renewSubscription(subscriptionId);
    }

    private SubscriptionManagerRestClient resolveSmClient(String subscriptionId) {
        Subscription subscription = subscriptionRepository.findBySubscriptionId(subscriptionId)
                .orElseThrow(() -> new IllegalStateException("Subscription not found: " + subscriptionId));
        ProviderConfiguration provider = providerConfigParser.findByProviderId(subscription.getProviderId())
                .orElseThrow(() -> new IllegalStateException("Provider not configured: " + subscription.getProviderId()));
        return smClientRegistry.getOrCreate(provider);
    }
}
