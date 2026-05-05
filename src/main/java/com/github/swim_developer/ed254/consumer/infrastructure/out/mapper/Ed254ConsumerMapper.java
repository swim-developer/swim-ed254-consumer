package com.github.swim_developer.ed254.consumer.infrastructure.out.mapper;

import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.EventDTO;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionDTO;
import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.framework.consumer.infrastructure.out.dlq.DeadLetterMessage;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.framework.infrastructure.out.messaging.DlqMessageDTO;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class Ed254ConsumerMapper {

    public SubscriptionDTO toDTO(Subscription subscription) {
        return new SubscriptionDTO(
                subscription.getId(),
                subscription.getSubscriptionId(),
                subscription.getSubscriptionStatus(),
                subscription.getQueueName(),
                subscription.extractAerodromeDesignators(),
                subscription.getPointName(),
                null,
                subscription.getDescription(),
                subscription.getSubscriptionEnd()
        );
    }

    public EventDTO toDTO(ArrivalEvent event) {
        return new EventDTO(
                event.getId() != null ? event.getId().toString() : null,
                event.getMessageId(),
                event.getAerodromeDesignator(),
                event.getMessageType(),
                event.getPublicationTime(),
                event.getSequenceEntriesCount(),
                event.getDeliveryStatus() != null ? event.getDeliveryStatus().name() : null,
                event.getReceivedAt(),
                event.getDispatchedAt()
        );
    }

    public DlqMessageDTO toDTO(DeadLetterMessage dlq) {
        return new DlqMessageDTO(
                dlq.getId(),
                dlq.getAmqpMessageId(),
                dlq.getMessageIndex(),
                dlq.getSubscriptionId(),
                dlq.getQueueName(),
                dlq.getErrorType(),
                dlq.getErrorMessage(),
                dlq.getRawPayload(),
                dlq.getReceivedAt(),
                dlq.getFailedAt()
        );
    }
}
