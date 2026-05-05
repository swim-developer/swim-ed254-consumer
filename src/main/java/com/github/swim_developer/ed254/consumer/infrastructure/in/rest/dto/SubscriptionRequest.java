package com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto;

import com.github.swim_developer.ed254.consumer.domain.model.SubscriptionFilters;
import com.github.swim_developer.ed254.consumer.domain.model.SupplementaryData;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public record SubscriptionRequest(
        SubscriptionFilters subscriptionFilters,
        SupplementaryData supplementaryData
) {}
