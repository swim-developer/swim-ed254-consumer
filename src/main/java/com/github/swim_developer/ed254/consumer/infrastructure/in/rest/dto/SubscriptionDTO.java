package com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto;

import com.github.swim_developer.ed254.consumer.domain.model.SupplementaryData;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.time.Instant;
import java.util.List;

@RegisterForReflection
@Schema(description = "Subscription information")
public record SubscriptionDTO(
        @Schema(description = "MongoDB document ID")
        String id,

        @Schema(description = "Subscription reference from provider (subscrReference)", required = true)
        String subscriptionId,

        @Schema(description = "Subscription status", required = true, enumeration = {"PAUSED", "ACTIVE"})
        String subscriptionStatus,

        @Schema(description = "AMQP queue name", required = true)
        String queueName,

        @Schema(description = "Destination aerodrome ICAO filters")
        List<String> aerodromes,

        @Schema(description = "Metering point name filters")
        List<String> pointNames,

        @Schema(description = "Supplementary data selection")
        SupplementaryData supplementaryData,

        @Schema(description = "Subscription description")
        String description,

        @Schema(description = "Subscription expiration timestamp")
        Instant subscriptionEnd
) {
}
