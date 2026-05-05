package com.github.swim_developer.ed254.consumer.domain.model;

import com.github.swim_developer.framework.domain.model.SwimConsumerSubscription;
import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@RegisterForReflection
public class Subscription implements SwimConsumerSubscription {

    private String id;
    private String subscriptionId;
    private String queueName;
    private String subscriptionStatus;
    private List<DestinationAerodrome> destinationAerodrome;
    private List<String> pointName;
    private List<FlightSelector> flightSelector;
    private boolean anySupplementaryData;
    private String description;
    private String type;
    private String configHash;
    private Instant subscriptionEnd;
    private String providerName;
    private String providerId;
    private String heartbeatQueue;

    public List<String> extractAerodromeDesignators() {
        if (destinationAerodrome == null || destinationAerodrome.isEmpty()) {
            return List.of();
        }
        return destinationAerodrome.stream()
                .map(DestinationAerodrome::aerodromeDesignator)
                .toList();
    }

    @Override
    public Map<String, Set<String>> projectFilterDimensions() {
        Map<String, Set<String>> dimensions = new HashMap<>();
        dimensions.put(FilterDimension.AERODROME, Set.copyOf(extractAerodromeDesignators()));
        dimensions.put(FilterDimension.SUPPLEMENTARY_DATA,
                Set.of(String.valueOf(anySupplementaryData)));
        return dimensions;
    }
}
