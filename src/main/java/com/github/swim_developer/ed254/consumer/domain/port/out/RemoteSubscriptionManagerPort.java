package com.github.swim_developer.ed254.consumer.domain.port.out;

import com.github.swim_developer.ed254.consumer.domain.model.DesiredSubscription;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;

public interface RemoteSubscriptionManagerPort {

    Subscription createSubscription(DesiredSubscription desired, ProviderConfiguration provider);

    String updateSubscriptionStatus(String subscriptionId, String newStatus, ProviderConfiguration provider);

    void deleteSubscription(String subscriptionId, ProviderConfiguration provider);

    void communicateProblems(String subscriptionId, DataValidationResult result, ProviderConfiguration provider);
}
