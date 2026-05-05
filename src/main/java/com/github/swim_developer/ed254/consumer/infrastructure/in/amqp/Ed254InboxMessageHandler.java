package com.github.swim_developer.ed254.consumer.infrastructure.in.amqp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.extension.inbox.reader.kafka.AbstractKafkaInboxReader;
import com.github.swim_developer.framework.application.model.PreparedEvent;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.infrastructure.out.messaging.InboxEnvelope;
import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecordBatch;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.util.List;
import java.util.concurrent.CompletionStage;
import com.github.swim_developer.ed254.consumer.application.usecase.Ed254EventProcessingUseCase;

@Slf4j
@ApplicationScoped
public class Ed254InboxMessageHandler extends AbstractKafkaInboxReader {

    private final Ed254EventProcessingUseCase eventProcessor;

    protected Ed254InboxMessageHandler() {
        this(null, null, null);
    }

    @Inject
    public Ed254InboxMessageHandler(ObjectMapper objectMapper, MeterRegistry meterRegistry,
                              Ed254EventProcessingUseCase eventProcessor) {
        super(objectMapper, meterRegistry);
        this.eventProcessor = eventProcessor;
    }

    @Incoming("in-ed254-inbox")
    @Blocking
    public CompletionStage<Void> onInboxBatch(KafkaRecordBatch<String, String> batch) {
        List<PreparedEvent<ArrivalEvent>> prepared = prepareBatch(batch, eventProcessor.eventProcessingOrchestrator());

        if (!prepared.isEmpty()) {
            eventProcessor.batchPersistAndDispatch(prepared);
            eventProcessor.markBatchAsProcessed(prepared);
        }

        processedCounter.increment(prepared.size());
        return batch.ack();
    }

    @Override
    public void processSingleMessage(InboxEnvelope envelope, String xmlPayload, int index) {
        ProcessingContext ctx = new ProcessingContext(
                envelope.subscriptionId(),
                envelope.queueName(),
                envelope.amqpMessageId(),
                xmlPayload,
                index,
                null);
        eventProcessor.processMessage(ctx);
    }

    @Override
    public String getMetricPrefix() {
        return "ed254";
    }
}
