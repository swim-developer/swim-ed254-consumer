package com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.time.Instant;
import java.util.List;

@RegisterForReflection
@JsonIgnoreProperties(ignoreUnknown = true)
public record SubscriptionResponse(
        @JsonAlias("subscrReference")
        String subscriptionId,

        String subscriptionResult,

        String errorReason,

        List<ErrorDetail> errorDetails,

        String queueName,

        String subscriptionStatus,

        Instant subscriptionEnd,

        String providerName,

        @JsonAlias("heartbeat_queue")
        String heartbeatQueue
) {}
