package com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.time.Instant;

@RegisterForReflection
@Schema(description = "ED-254 arrival event")
public record EventDTO(
        @Schema(description = "MongoDB document ID")
        String id,

        @Schema(description = "Message ID from AMQP", required = true)
        String messageId,

        @Schema(description = "Aerodrome ICAO designator")
        String aerodromeDesignator,

        @Schema(description = "Message type")
        String messageType,

        @Schema(description = "Publication time from provider")
        Instant publicationTime,

        @Schema(description = "Number of sequence entries in this message")
        int sequenceEntriesCount,

        @Schema(description = "Kafka delivery status")
        String kafkaStatus,

        @Schema(description = "Timestamp when message was received from AMQP", required = true)
        Instant receivedAt,

        @Schema(description = "Timestamp when message was sent to Kafka")
        Instant sentToKafkaAt
) {
}
