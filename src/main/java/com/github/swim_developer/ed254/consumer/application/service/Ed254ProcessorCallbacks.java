package com.github.swim_developer.ed254.consumer.application.service;

import aero.fixm.ed254.ArrivalSequence;
import com.github.swim_developer.ed254.consumer.application.port.in.Ed254ProcessingConfig;
import com.github.swim_developer.ed254.consumer.application.port.out.Ed254SequenceGapPort;
import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.model.Ed254Message;
import com.github.swim_developer.ed254.consumer.domain.port.out.RemoteSubscriptionManagerPort;
import com.github.swim_developer.ed254.consumer.domain.port.out.SubscriptionStore;
import com.github.swim_developer.framework.application.model.ProcessingContext;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventProcessorCallbacks;
import com.github.swim_developer.framework.consumer.application.messaging.processing.SwimEventProcessorConfig;
import com.github.swim_developer.framework.domain.exception.EventProcessingException;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import com.github.swim_developer.framework.domain.model.ErrorCode;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import com.github.swim_developer.framework.application.port.out.SwimDeadLetterPort;
import com.github.swim_developer.framework.application.port.out.SwimProviderConfigPort;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@ApplicationScoped
public class Ed254ProcessorCallbacks implements SwimEventProcessorCallbacks<ArrivalEvent> {

    private final Ed254ProcessingConfig processingConfig;
    private final Ed254SequenceGapPort sequenceGapDetector;
    private final RemoteSubscriptionManagerPort smPort;
    private final SwimProviderConfigPort providerConfigParser;
    private final SubscriptionStore subscriptionRepository;
    private final Vertx vertx;
    private final Ed254ParsedMessageHolder parsedMessageHolder;
    private final SwimDeadLetterPort deadLetterService;

    @Inject
    public Ed254ProcessorCallbacks(Ed254ProcessingConfig processingConfig,
                                  Ed254SequenceGapPort sequenceGapDetector,
                                  RemoteSubscriptionManagerPort smPort,
                                  SwimProviderConfigPort providerConfigParser,
                                  SubscriptionStore subscriptionRepository,
                                  Vertx vertx,
                                  Ed254ParsedMessageHolder parsedMessageHolder,
                                  SwimDeadLetterPort deadLetterService) {
        this.processingConfig = processingConfig;
        this.sequenceGapDetector = sequenceGapDetector;
        this.smPort = smPort;
        this.providerConfigParser = providerConfigParser;
        this.subscriptionRepository = subscriptionRepository;
        this.vertx = vertx;
        this.parsedMessageHolder = parsedMessageHolder;
        this.deadLetterService = deadLetterService;
    }

    @Override
    public void onValidationFailure(ProcessingContext ctx, Exception e) {
        reportProblemToProvider(ctx.subscriptionId(), DataValidationResult.wrongFormat(e.getMessage()));
    }

    @Override
    public void onExtractionFailure(ProcessingContext ctx, SwimEventProcessorConfig config) {
        reportProblemToProvider(ctx.subscriptionId(), DataValidationResult.dataInvalid(
                ErrorCode.NOT_READABLE, null, "Failed to extract ED-254 event metadata"));
        config.getDeadLetterService().sendToDeadLetterQueue(
                ctx.subscriptionId(), ctx.queueName(), ctx.amqpMessageId(),
                ctx.index(), ctx.xmlPayload(), "EXTRACTION_ERROR",
                new IllegalArgumentException("Failed to extract event metadata"));
        throw new EventProcessingException("Failed to extract ed254 event");
    }

    @Override
    public boolean postExtractValidation(ProcessingContext ctx, ArrivalEvent event) {
        if (!isTimestampFresh(event.getCreationTime())) {
            long ageMs = Duration.between(event.getCreationTime(), Instant.now()).toMillis();
            log.warn("ED-254 message too old - MessageId: {}, CreationTime: {}, AgeMs: {}, ThresholdMs: {}",
                    ctx.amqpMessageId(), event.getCreationTime(), ageMs, processingConfig.messageValidityThresholdMs());
            reportProblemToProvider(ctx.subscriptionId(), DataValidationResult.dataInvalid(
                    ErrorCode.TIME_ACCURACY, "creationTime",
                    event.getCreationTime() + " exceeds " + processingConfig.messageValidityThresholdMs() + "ms threshold"));
            deadLetterService.sendToDeadLetterQueue(
                    ctx.subscriptionId(), ctx.queueName(), ctx.amqpMessageId(), ctx.index(), ctx.xmlPayload(), "STALE_MESSAGE",
                    new IllegalArgumentException("Message age " + ageMs + "ms exceeds threshold "
                            + processingConfig.messageValidityThresholdMs() + "ms"));
            return false;
        }

        Ed254Message parsed = parsedMessageHolder.get();
        parsedMessageHolder.remove();
        if (parsed instanceof Ed254Message.ArrivalMsg(ArrivalSequence seq)) {
            sequenceGapDetector.detect(ctx.queueName(), seq)
                    .ifPresent(result -> reportProblemToProvider(ctx.subscriptionId(), result));
        }

        return true;
    }

    private boolean isTimestampFresh(Instant creationTime) {
        if (creationTime == null) {
            return true;
        }
        return creationTime.plusMillis(processingConfig.messageValidityThresholdMs()).isAfter(Instant.now());
    }

    private void reportProblemToProvider(String subscriptionId, DataValidationResult result) {
        vertx.executeBlocking(() -> {
            ProviderConfiguration provider = resolveProviderForSubscription(subscriptionId);
            smPort.communicateProblems(subscriptionId, result, provider);
            return null;
        }).onFailure(e -> log.debug("Failed to report problem to provider for subscription {}: {}",
                subscriptionId, e.getMessage()));
    }

    private ProviderConfiguration resolveProviderForSubscription(String subscriptionId) {
        return subscriptionRepository.findBySubscriptionId(subscriptionId)
                .map(sub -> providerConfigParser.findByProviderId(sub.getProviderId())
                        .orElseThrow(() -> new IllegalStateException("Provider not configured: " + sub.getProviderId())))
                .orElseThrow(() -> new IllegalStateException("Subscription not found: " + subscriptionId));
    }
}
