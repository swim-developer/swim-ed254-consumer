package com.github.swim_developer.ed254.consumer.application.port.in;

import com.github.swim_developer.ed254.consumer.domain.model.DesiredSubscription;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;

import java.util.Optional;

public interface ManageSubscriptionPort {

    Subscription createSubscription(DesiredSubscription desired);

    Optional<Subscription> findBySubscriptionId(String subscriptionId);

    void deleteSubscriptionById(String subscriptionId);

    Subscription pauseSubscription(String subscriptionId);

    Subscription resumeSubscription(String subscriptionId);

    ProviderConfiguration resolveProvider(String providerId);
}
