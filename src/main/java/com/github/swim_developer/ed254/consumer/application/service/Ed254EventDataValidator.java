package com.github.swim_developer.ed254.consumer.application.service;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.model.FilterDimension;
import com.github.swim_developer.ed254.consumer.domain.port.out.RemoteSubscriptionManagerPort;
import com.github.swim_developer.ed254.consumer.domain.port.out.SubscriptionStore;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventValidator;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import com.github.swim_developer.framework.application.port.out.SwimProviderConfigPort;
import com.github.swim_developer.framework.application.port.out.SwimSubscriptionFilterPort;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class Ed254EventDataValidator implements SwimEventValidator<ArrivalEvent> {

    private final SwimSubscriptionFilterPort filterCache;
    private final RemoteSubscriptionManagerPort smPort;
    private final SwimProviderConfigPort providerConfigParser;
    private final SubscriptionStore subscriptionRepository;
    private final Vertx vertx;

    @Inject
    public Ed254EventDataValidator(SwimSubscriptionFilterPort filterCache,
                                  RemoteSubscriptionManagerPort smPort,
                                  SwimProviderConfigPort providerConfigParser,
                                  SubscriptionStore subscriptionRepository,
                                  Vertx vertx) {
        this.filterCache = filterCache;
        this.smPort = smPort;
        this.providerConfigParser = providerConfigParser;
        this.subscriptionRepository = subscriptionRepository;
        this.vertx = vertx;
    }

    @Override
    public void validateExtractedData(ProcessingContext ctx, ArrivalEvent event) {
        if (!isSupplementaryDataSubscribed(ctx.subscriptionId())
                && containsSupplementaryData(ctx.xmlPayload())) {
            log.warn("NON_SUBSCRIBED_DATA: Supplementary data present but not subscribed - SubscriptionId: {}, MessageId: {}",
                    ctx.subscriptionId(), ctx.amqpMessageId());
            reportProblemToProvider(ctx.subscriptionId(), DataValidationResult.nonSubscribedData(
                    "Received supplementary data for subscription that did not request it"));
        }
    }

    private boolean isSupplementaryDataSubscribed(String subscriptionId) {
        return filterCache.isAllowed(subscriptionId, FilterDimension.SUPPLEMENTARY_DATA, "true");
    }

    private boolean containsSupplementaryData(String xml) {
        if (xml == null) {
            return false;
        }
        return xml.contains("arrivalDelay")
                || xml.contains("landingSequencePosition")
                || xml.contains("amanStrategy")
                || xml.contains("departureAerodrome")
                || xml.contains("proposedProcedure");
    }

    private void reportProblemToProvider(String subscriptionId, DataValidationResult result) {
        vertx.executeBlocking(() -> {
            ProviderConfiguration provider = resolveProviderForSubscription(subscriptionId);
            smPort.communicateProblems(subscriptionId, result, provider);
            return null;
        }).onFailure(e -> log.debug("Failed to report problem to provider for subscription {}: {}",
                subscriptionId, e.getMessage()));
    }

    private ProviderConfiguration resolveProviderForSubscription(String subscriptionId) {
        return subscriptionRepository.findBySubscriptionId(subscriptionId)
                .map(sub -> providerConfigParser.findByProviderId(sub.getProviderId())
                        .orElseThrow(() -> new IllegalStateException("Provider not configured: " + sub.getProviderId())))
                .orElseThrow(() -> new IllegalStateException("Subscription not found: " + subscriptionId));
    }
}
