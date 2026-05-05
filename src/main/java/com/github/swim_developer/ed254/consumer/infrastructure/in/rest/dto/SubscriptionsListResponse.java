package com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto;

import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public record SubscriptionsListResponse(List<SubscriptionResponse> subscriptions) {
}
