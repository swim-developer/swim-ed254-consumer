package com.github.swim_developer.ed254.consumer.domain.model;


import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

@RegisterForReflection
public record SubscriptionFilters(
        List<DestinationAerodrome> destinationAerodrome,
        List<String> pointName,
        List<FlightSelector> flightSelector
) {}
