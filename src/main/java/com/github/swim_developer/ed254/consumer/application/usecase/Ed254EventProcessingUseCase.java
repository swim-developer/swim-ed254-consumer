package com.github.swim_developer.ed254.consumer.application.usecase;

import com.github.swim_developer.ed254.consumer.application.service.Ed254EventDataValidator;
import com.github.swim_developer.ed254.consumer.infrastructure.out.xml.Ed254EventExtractor;
import com.github.swim_developer.ed254.consumer.application.service.Ed254EventFilterService;
import com.github.swim_developer.ed254.consumer.application.service.Ed254EventParserAdapter;
import com.github.swim_developer.ed254.consumer.application.service.Ed254EventPersistenceService;
import com.github.swim_developer.ed254.consumer.application.service.Ed254ProcessorCallbacks;
import com.github.swim_developer.framework.consumer.application.messaging.processing.DefaultEventProcessorConfig;
import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.model.Ed254Message;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.document.ArrivalEventDocument;
import com.github.swim_developer.framework.application.model.PreparedEvent;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.application.model.ProcessingOutcome;
import com.github.swim_developer.framework.consumer.application.messaging.outbox.AbstractOutboxEventConsumer;
import com.github.swim_developer.framework.consumer.application.messaging.processing.EventProcessingOrchestrator;
import com.github.swim_developer.framework.consumer.application.messaging.processing.EventProcessingOrchestratorDependencies;
import com.github.swim_developer.framework.application.port.in.SwimMessageInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import java.util.List;

@ApplicationScoped
public class Ed254EventProcessingUseCase {

    private final EventProcessingOrchestrator<ArrivalEvent, Ed254Message> orchestrator;
    private final Ed254EventPersistenceService persistenceService;
    private final AbstractOutboxEventConsumer<ArrivalEventDocument> outboxEventConsumer;

    @Inject
    public Ed254EventProcessingUseCase(
            DefaultEventProcessorConfig processorConfig,
            Ed254EventParserAdapter eventParser,
            Ed254EventExtractor eventExtractor,
            Ed254EventDataValidator validator,
            Ed254EventFilterService filterService,
            Ed254EventPersistenceService persistenceService,
            Ed254ProcessorCallbacks callbacks,
            AbstractOutboxEventConsumer<ArrivalEventDocument> outboxEventConsumer,
            MeterRegistry meterRegistry,
            @Any Instance<SwimMessageInterceptor> interceptorInstances) {
        this.outboxEventConsumer = outboxEventConsumer;
        this.persistenceService = persistenceService;
        this.orchestrator = new EventProcessingOrchestrator<>(new EventProcessingOrchestratorDependencies<>(
                processorConfig, eventParser, eventExtractor, validator, filterService,
                persistenceService, callbacks, meterRegistry, interceptorInstances));
    }

    public ProcessingOutcome processAndPersist(String subscriptionId, String queueName, String amqpMessageId,
                                               String xmlPayload) {
        return orchestrator.processMessage(new ProcessingContext(subscriptionId, queueName, amqpMessageId, xmlPayload, 0, null));
    }

    public void retryPendingKafkaEvents(ArrivalEvent event) {
        outboxEventConsumer.processOutboxEvent(event.getId());
    }

    public EventProcessingOrchestrator<ArrivalEvent, Ed254Message> eventProcessingOrchestrator() {
        return orchestrator;
    }

    public ProcessingOutcome processMessage(ProcessingContext ctx) {
        return orchestrator.processMessage(ctx);
    }

    public void batchPersistAndDispatch(List<PreparedEvent<ArrivalEvent>> batch) {
        persistenceService.batchPersistAndDispatch(batch);
    }

    public void markBatchAsProcessed(List<PreparedEvent<ArrivalEvent>> batch) {
        orchestrator.markBatchAsProcessed(batch);
    }
}
