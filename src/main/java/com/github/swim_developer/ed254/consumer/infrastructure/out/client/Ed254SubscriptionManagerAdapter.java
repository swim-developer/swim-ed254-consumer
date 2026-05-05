package com.github.swim_developer.ed254.consumer.infrastructure.out.client;

import com.github.swim_developer.ed254.consumer.domain.model.DesiredSubscription;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.domain.model.SubscriptionFilters;
import com.github.swim_developer.ed254.consumer.domain.port.out.RemoteSubscriptionManagerPort;
import com.github.swim_developer.framework.consumer.application.port.out.SwimRemoteFeatureQueryPort;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionRequest;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionResponse;
import com.github.swim_developer.framework.consumer.infrastructure.out.client.AbstractSubscriptionManagerClientRegistry;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class Ed254SubscriptionManagerAdapter
        extends AbstractSubscriptionManagerClientRegistry<SubscriptionManagerRestClient>
        implements RemoteSubscriptionManagerPort, SwimRemoteFeatureQueryPort {

    @Override
    protected Class<SubscriptionManagerRestClient> getClientClass() {
        return SubscriptionManagerRestClient.class;
    }

    @Override
    public Subscription createSubscription(DesiredSubscription desired, ProviderConfiguration provider) {
        SubscriptionManagerRestClient client = getOrCreate(provider);
        SubscriptionRequest request = toRequest(desired);
        SubscriptionResponse response = executeWithRetry(provider, "createSubscription",
                () -> client.createSubscription(request));
        return fromResponse(response, desired);
    }

    @Override
    public String updateSubscriptionStatus(String subscriptionId, String newStatus, ProviderConfiguration provider) {
        SubscriptionManagerRestClient client = getOrCreate(provider);
        SubscriptionResponse response;
        if ("PAUSED".equalsIgnoreCase(newStatus)) {
            response = client.pauseSubscription(subscriptionId);
        } else {
            response = client.resumeSubscription(subscriptionId);
        }
        return response != null && response.subscriptionStatus() != null
                ? response.subscriptionStatus()
                : newStatus;
    }

    @Override
    public void deleteSubscription(String subscriptionId, ProviderConfiguration provider) {
        SubscriptionManagerRestClient client = getOrCreate(provider);
        client.deleteSubscription(subscriptionId);
    }

    @Override
    public String queryFeatures(String typeName, String filter, String validTime, ProviderConfiguration provider) {
        SubscriptionManagerRestClient client = getOrCreate(provider);
        return executeWithRetry(provider, "getFeatures",
                () -> client.getFeatures(typeName, filter, null, validTime, null, null, null, null));
    }

    @Override
    public void communicateProblems(String subscriptionId, DataValidationResult result, ProviderConfiguration provider) {
        SubscriptionManagerRestClient client = getOrCreate(provider);
        client.communicateProblems(result);
    }

    @Override
    public String querySubscriptionStatus(String subscriptionId, ProviderConfiguration provider) {
        SubscriptionManagerRestClient client = getOrCreate(provider);
        SubscriptionResponse response = client.getSubscriptionDetails(subscriptionId);
        return response != null && response.subscriptionStatus() != null
                ? response.subscriptionStatus()
                : "UNKNOWN";
    }

    private static SubscriptionRequest toRequest(DesiredSubscription desired) {
        SubscriptionFilters filters = new SubscriptionFilters(
                desired.destinationAerodrome(),
                desired.pointName(),
                desired.flightSelector()
        );
        return new SubscriptionRequest(filters, desired.supplementaryData());
    }

    private static Subscription fromResponse(SubscriptionResponse response, DesiredSubscription desired) {
        Subscription sub = new Subscription();
        sub.setSubscriptionId(response.subscriptionId());
        sub.setQueueName(response.queueName());
        sub.setSubscriptionStatus(response.subscriptionStatus());
        sub.setSubscriptionEnd(response.subscriptionEnd());
        sub.setProviderName(response.providerName());
        sub.setProviderId(desired.provider());
        sub.setHeartbeatQueue(response.heartbeatQueue());
        sub.setDestinationAerodrome(desired.destinationAerodrome());
        sub.setPointName(desired.pointName());
        sub.setFlightSelector(desired.flightSelector());
        sub.setAnySupplementaryData(desired.supplementaryData() != null && desired.supplementaryData().anyRequested());
        sub.setDescription(desired.description());
        return sub;
    }
}
