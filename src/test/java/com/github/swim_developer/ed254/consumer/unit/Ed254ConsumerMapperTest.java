package com.github.swim_developer.ed254.consumer.unit;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.model.DestinationAerodrome;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.EventDTO;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionDTO;
import com.github.swim_developer.ed254.consumer.infrastructure.out.mapper.Ed254ConsumerMapper;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.consumer.infrastructure.out.dlq.DeadLetterMessage;
import com.github.swim_developer.framework.infrastructure.out.messaging.DlqMessageDTO;
import com.github.swim_developer.framework.infrastructure.testing.TestNameLoggerExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestNameLoggerExtension.class)
class Ed254ConsumerMapperTest {

    private Ed254ConsumerMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new Ed254ConsumerMapper();
    }

    @Test
    void toDTO_subscription_mapsAllFields() {
        Instant end = Instant.parse("2026-06-01T00:00:00Z");
        Subscription sub = new Subscription();
        sub.setId("mongo-id-1");
        sub.setSubscriptionId("sub-abc");
        sub.setSubscriptionStatus("ACTIVE");
        sub.setQueueName("ED254.v1.user.sub-abc");
        sub.setDestinationAerodrome(List.of(new DestinationAerodrome("LPPT", null), new DestinationAerodrome("EHAM", null)));
        sub.setDescription("Test subscription");
        sub.setSubscriptionEnd(end);

        SubscriptionDTO dto = mapper.toDTO(sub);

        assertThat(dto.id()).isEqualTo("mongo-id-1");
        assertThat(dto.subscriptionId()).isEqualTo("sub-abc");
        assertThat(dto.subscriptionStatus()).isEqualTo("ACTIVE");
        assertThat(dto.queueName()).isEqualTo("ED254.v1.user.sub-abc");
        assertThat(dto.aerodromes()).containsExactly("LPPT", "EHAM");
        assertThat(dto.description()).isEqualTo("Test subscription");
        assertThat(dto.subscriptionEnd()).isEqualTo(end);
    }

    @Test
    void toDTO_subscription_withNullAerodromes_returnsEmptyList() {
        Subscription sub = new Subscription();
        sub.setDestinationAerodrome(null);

        SubscriptionDTO dto = mapper.toDTO(sub);

        assertThat(dto.aerodromes()).isEmpty();
    }

    @Test
    void toDTO_arrivalEvent_mapsAllFields() {
        Instant publication = Instant.parse("2026-03-15T12:00:00Z");
        Instant received = Instant.parse("2026-03-15T12:00:01Z");
        Instant dispatched = Instant.parse("2026-03-15T12:00:02Z");
        ArrivalEvent event = new ArrivalEvent();
        event.setId("event-id-1");
        event.setMessageId("msg-xyz");
        event.setAerodromeDesignator("LPPT");
        event.setMessageType("SEQUENCE");
        event.setPublicationTime(publication);
        event.setSequenceEntriesCount(5);
        event.setDeliveryStatus(OutboxDeliveryStatus.SENT);
        event.setReceivedAt(received);
        event.setDispatchedAt(dispatched);

        EventDTO dto = mapper.toDTO(event);

        assertThat(dto.id()).isEqualTo("event-id-1");
        assertThat(dto.messageId()).isEqualTo("msg-xyz");
        assertThat(dto.aerodromeDesignator()).isEqualTo("LPPT");
        assertThat(dto.messageType()).isEqualTo("SEQUENCE");
        assertThat(dto.publicationTime()).isEqualTo(publication);
        assertThat(dto.sequenceEntriesCount()).isEqualTo(5);
        assertThat(dto.kafkaStatus()).isEqualTo("SENT");
        assertThat(dto.receivedAt()).isEqualTo(received);
        assertThat(dto.sentToKafkaAt()).isEqualTo(dispatched);
    }

    @Test
    void toDTO_arrivalEvent_withNullIdAndStatus_returnsNulls() {
        ArrivalEvent event = new ArrivalEvent();
        event.setId(null);
        event.setDeliveryStatus(null);

        EventDTO dto = mapper.toDTO(event);

        assertThat(dto.id()).isNull();
        assertThat(dto.kafkaStatus()).isNull();
    }

    @Test
    void toDTO_deadLetterMessage_mapsAllFields() {
        Instant received = Instant.parse("2026-04-01T06:00:00Z");
        Instant failed = Instant.parse("2026-04-01T06:00:10Z");
        DeadLetterMessage dlq = new DeadLetterMessage(
                "dlq-1", "amqp-1", 3, "sub-1", "queue-1",
                "<xml/>", "PERSISTENCE_ERROR", "mongo timeout", "stack...",
                received, failed);

        DlqMessageDTO dto = mapper.toDTO(dlq);

        assertThat(dto.id()).isEqualTo("dlq-1");
        assertThat(dto.amqpMessageId()).isEqualTo("amqp-1");
        assertThat(dto.messageIndex()).isEqualTo(3);
        assertThat(dto.subscriptionId()).isEqualTo("sub-1");
        assertThat(dto.errorType()).isEqualTo("PERSISTENCE_ERROR");
        assertThat(dto.rawPayload()).isEqualTo("<xml/>");
        assertThat(dto.receivedAt()).isEqualTo(received);
        assertThat(dto.failedAt()).isEqualTo(failed);
    }
}
