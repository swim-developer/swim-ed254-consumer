package com.github.swim_developer.ed254.consumer.application.service;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.port.out.ArrivalEventStore;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.application.port.out.SwimDeadLetterPort;
import com.github.swim_developer.framework.consumer.application.messaging.outbox.OutboxRouterFanOut;
import com.github.swim_developer.framework.consumer.application.messaging.processing.AbstractEventPersistenceService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.List;

@ApplicationScoped
public class Ed254EventPersistenceService extends AbstractEventPersistenceService<ArrivalEvent, ArrivalEvent> {

    private final ArrivalEventStore eventRepository;

    @Inject
    public Ed254EventPersistenceService(ArrivalEventStore eventRepository,
                                       OutboxRouterFanOut outboxRouterFanOut,
                                       SwimDeadLetterPort deadLetterService) {
        super(outboxRouterFanOut, deadLetterService);
        this.eventRepository = eventRepository;
    }

    @Override
    protected ArrivalEvent assembleEntity(ProcessingContext ctx, ArrivalEvent event, String contentHash) {
        event.setSubscriptionId(ctx.subscriptionId());
        event.setMessageId(ctx.amqpMessageId());
        event.setRawPayload(ctx.xmlPayload());
        event.setContentHash(contentHash);
        event.setDeliveryStatus(OutboxDeliveryStatus.SENT);
        event.setDispatchedAt(Instant.now());
        return event;
    }

    @Override
    protected void persistEntity(ArrivalEvent entity) { eventRepository.persist(entity); }

    @Override
    protected void persistEntities(List<ArrivalEvent> entities) { eventRepository.persist(entities); }

    @Override
    protected void updateEntity(ArrivalEvent entity) { eventRepository.update(entity); }

    @Override
    protected String getServicePrefix() { return "ED-254"; }
}
