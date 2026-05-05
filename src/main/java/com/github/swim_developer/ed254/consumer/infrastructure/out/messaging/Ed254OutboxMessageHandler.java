package com.github.swim_developer.ed254.consumer.infrastructure.out.messaging;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoArrivalEventStore;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document.ArrivalEventDocument;
import com.github.swim_developer.framework.consumer.application.messaging.outbox.AbstractOutboxEventConsumer;
import com.github.swim_developer.framework.consumer.application.messaging.outbox.OutboxRouterFanOut;
import com.github.swim_developer.framework.consumer.application.port.out.SwimOutboxRetryPort;
import com.github.swim_developer.framework.domain.model.SwimOutboxEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@Slf4j
@ApplicationScoped
public class Ed254OutboxMessageHandler extends AbstractOutboxEventConsumer<ArrivalEventDocument> implements SwimOutboxRetryPort {

    private final MongoArrivalEventStore eventRepository;
    private final OutboxRouterFanOut outboxRouterFanOut;

    @Inject
    public Ed254OutboxMessageHandler(
            MongoArrivalEventStore eventRepository,
            OutboxRouterFanOut outboxRouterFanOut,
            @ConfigProperty(name = "swim.outbox.kafka.max-retries", defaultValue = "3") int maxKafkaRetries) {
        super(maxKafkaRetries);
        this.eventRepository = eventRepository;
        this.outboxRouterFanOut = outboxRouterFanOut;
    }

    public void retrySend(ArrivalEvent event) {
        sendAndUpdateStatus(ArrivalEventDocument.fromDomain(event));
    }

    @Override
    public void retryOutboxEvent(SwimOutboxEvent event) {
        sendAndUpdateStatus(ArrivalEventDocument.fromDomain((ArrivalEvent) event));
    }

    @Override
    protected ArrivalEventDocument resolveEvent(String eventIdStr) {
        return eventRepository.findEventDocumentById(eventIdStr);
    }

    @Override
    protected OutboxRouterFanOut getRouterFanOut() {
        return outboxRouterFanOut;
    }

    @Override
    protected String getEventId(ArrivalEventDocument event) {
        return event.getId() != null ? event.getId().toHexString() : null;
    }

    @Override
    protected void updateEvent(ArrivalEventDocument event) {
        eventRepository.persistOrUpdate(event);
    }

}
