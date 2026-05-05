package com.github.swim_developer.ed254.consumer.unit;

import aero.fixm.ed254.ArrivalSequence;
import aero.fixm.ed254.ProviderExceptions;
import com.github.swim_developer.ed254.consumer.infrastructure.out.client.Ed254SubscriptionManagerAdapter;
import com.github.swim_developer.ed254.consumer.infrastructure.out.client.SubscriptionManagerRestClient;
import com.github.swim_developer.ed254.consumer.domain.model.DestinationAerodrome;
import com.github.swim_developer.ed254.consumer.infrastructure.in.rest.dto.SubscriptionRequest;
import com.github.swim_developer.ed254.consumer.domain.model.SupplementaryData;
import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.ed254.consumer.domain.model.DesiredSubscription;
import com.github.swim_developer.ed254.consumer.domain.model.Ed254Message;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoArrivalEventStore;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoSubscriptionStore;
import com.github.swim_developer.framework.application.port.out.SwimDeadLetterPort;
import com.github.swim_developer.ed254.consumer.infrastructure.out.xml.Ed254EventExtractor;
import com.github.swim_developer.framework.application.port.out.SwimXmlUnmarshallerPort;
import com.github.swim_developer.ed254.consumer.application.port.in.Ed254ProcessingConfig;
import com.github.swim_developer.ed254.consumer.application.service.Ed254EventDataValidator;
import com.github.swim_developer.ed254.consumer.application.service.Ed254EventFilterService;
import com.github.swim_developer.ed254.consumer.application.service.Ed254EventParserAdapter;
import com.github.swim_developer.ed254.consumer.application.service.Ed254EventPersistenceService;
import com.github.swim_developer.ed254.consumer.application.service.Ed254ParsedMessageHolder;
import com.github.swim_developer.ed254.consumer.application.service.Ed254ProcessorCallbacks;
import com.github.swim_developer.framework.consumer.application.messaging.processing.DefaultEventProcessorConfig;
import com.github.swim_developer.ed254.consumer.application.usecase.Ed254EventProcessingUseCase;
import com.github.swim_developer.framework.consumer.infrastructure.out.idempotency.AbstractIdempotencyCache;
import com.github.swim_developer.ed254.consumer.infrastructure.out.messaging.Ed254OutboxMessageHandler;
import com.github.swim_developer.ed254.consumer.infrastructure.out.messaging.Ed254SequenceGapDetector;
import com.github.swim_developer.framework.application.port.out.SwimProviderConfigPort;
import com.github.swim_developer.framework.consumer.infrastructure.out.filter.SubscriptionFilterCache;
import com.github.swim_developer.framework.application.model.AmqpBrokerConfig;
import com.github.swim_developer.framework.domain.model.DataValidationResult;
import com.github.swim_developer.framework.application.model.ProviderConfiguration;
import com.github.swim_developer.framework.application.model.ResilienceConfig;
import com.github.swim_developer.framework.application.model.SubscriptionManagerConfig;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.application.port.in.SwimMessageInterceptor;
import com.github.swim_developer.framework.consumer.application.messaging.outbox.OutboxRouterFanOut;
import com.github.swim_developer.framework.application.model.ProcessingOutcome;
import com.github.swim_developer.framework.domain.exception.XmlValidationException;
import com.github.swim_developer.framework.infrastructure.util.HashUtil;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class Ed254ConsumerTest {

    private static final String VALID_ARRIVAL_SEQUENCE_XML = """
            <arrivalSequence xmlns="http://www.eurocae.net/ED-254">
                <aerodromeDesignator>ESSA</aerodromeDesignator>
                <publicationTime>%s</publicationTime>
                <creationTime>%s</creationTime>
                <firstMessageAfterServiceOutage>false</firstMessageAfterServiceOutage>
            </arrivalSequence>
            """.formatted(Instant.now().toString(), Instant.now().toString());

    private static final String VALID_PROVIDER_EXCEPTION_XML = """
            <providerExceptions xmlns="http://www.eurocae.net/ED-254">
                <provException>Test exception</provException>
            </providerExceptions>
            """;

    private static final long THRESHOLD_MS = 30_000;

    private Ed254EventProcessingUseCase eventProcessor;
    private MongoArrivalEventStore repository;
    private AbstractIdempotencyCache idempotencyCache;
    private SwimDeadLetterPort deadLetterService;
    private OutboxRouterFanOut outboxRouterFanOut;
    private SwimXmlUnmarshallerPort<Ed254Message> jaxbPool;
    private SubscriptionManagerRestClient providerClient;
    private Ed254SubscriptionManagerAdapter smClientRegistry;
    private SwimProviderConfigPort providerConfigParser;
    private Ed254OutboxMessageHandler outboxEventConsumer;
    private MongoSubscriptionStore subscriptionRepository;
    private Ed254EventExtractor eventExtractor;
    private SimpleMeterRegistry meterRegistry;
    private SubscriptionFilterCache filterCache;
    private Ed254SequenceGapDetector sequenceGapDetector;
    private Vertx vertx;

    @BeforeEach
    void setup(TestInfo testInfo) throws Exception {
        System.out.printf("%n══ ▶ %s.%s%n", getClass().getSimpleName(), testInfo.getDisplayName());
        meterRegistry = new SimpleMeterRegistry();
        eventExtractor = new Ed254EventExtractor();

        repository = mock(MongoArrivalEventStore.class);
        idempotencyCache = mock(AbstractIdempotencyCache.class);
        deadLetterService = mock(SwimDeadLetterPort.class);
        outboxRouterFanOut = mock(OutboxRouterFanOut.class);
        jaxbPool = mock(SwimXmlUnmarshallerPort.class);
        providerClient = mock(SubscriptionManagerRestClient.class);
        smClientRegistry = mock(Ed254SubscriptionManagerAdapter.class);
        providerConfigParser = mock(SwimProviderConfigPort.class);
        when(smClientRegistry.getOrCreate(any(ProviderConfiguration.class))).thenReturn(providerClient);
        outboxEventConsumer = mock(Ed254OutboxMessageHandler.class);
        subscriptionRepository = mock(MongoSubscriptionStore.class);

        var stubDoc = new com.github.swim_developer.ed254.consumer.domain.model.Subscription();
        stubDoc.setSubscriptionId("sub-1");
        stubDoc.setProviderId("test-provider");
        when(subscriptionRepository.findBySubscriptionId(anyString())).thenReturn(Optional.of(stubDoc));
        var testProvider = ProviderConfiguration.builder()
                .providerId("test-provider")
                .subscriptionManager(SubscriptionManagerConfig.builder()
                        .url("http://localhost:8081/swim/v1")
                        .tls(null)
                        .resilience(ResilienceConfig.builder()
                                .connectTimeoutMs(0).readTimeoutMs(0)
                                .retryMaxAttempts(0).retryDelayMs(0L).build())
                        .build())
                .amqpBroker(AmqpBrokerConfig.builder()
                        .host("localhost").port(5672).sslEnabled(false)
                        .username("guest").password("guest").tls(null).build())
                .build();
        when(providerConfigParser.findByProviderId("test-provider")).thenReturn(Optional.of(testProvider));
        when(providerConfigParser.findByProviderIdOrDefault("test-provider")).thenReturn(Optional.of(testProvider));
        sequenceGapDetector = mock(Ed254SequenceGapDetector.class);
        when(sequenceGapDetector.detect(anyString(), any(ArrivalSequence.class))).thenReturn(Optional.empty());

        vertx = mock(Vertx.class);
        when(vertx.executeBlocking(any(Callable.class))).thenAnswer(invocation -> {
            Callable<?> callable = invocation.getArgument(0);
            try {
                callable.call();
                return Future.succeededFuture();
            } catch (Exception e) {
                return Future.failedFuture(e);
            }
        });

        Instance<SwimMessageInterceptor> interceptorInstances = mock(Instance.class);
        when(interceptorInstances.isUnsatisfied()).thenReturn(true);

        when(jaxbPool.unmarshalAndValidate(VALID_ARRIVAL_SEQUENCE_XML))
                .thenReturn(buildArrivalMsg("ESSA", Instant.now(), Instant.now(), false));
        when(jaxbPool.unmarshalAndValidate(VALID_PROVIDER_EXCEPTION_XML))
                .thenReturn(buildExceptionMsg("Test exception"));

        filterCache = new SubscriptionFilterCache();
        Ed254ProcessingConfig processingConfig = mock(Ed254ProcessingConfig.class);
        when(processingConfig.messageValidityThresholdMs()).thenReturn(THRESHOLD_MS);
        DefaultEventProcessorConfig processorConfig = new DefaultEventProcessorConfig("ed254", idempotencyCache, deadLetterService);
        Ed254ParsedMessageHolder parsedMessageHolder = new Ed254ParsedMessageHolder();
        Ed254EventParserAdapter parserAdapter = new Ed254EventParserAdapter(jaxbPool, parsedMessageHolder);
        Ed254EventDataValidator validator = new Ed254EventDataValidator(
                filterCache, smClientRegistry, providerConfigParser, subscriptionRepository, vertx);
        Ed254EventFilterService filterService = new Ed254EventFilterService(filterCache, deadLetterService);
        Ed254EventPersistenceService persistenceService = new Ed254EventPersistenceService(
                repository, outboxRouterFanOut, deadLetterService);
        Ed254ProcessorCallbacks callbacks = new Ed254ProcessorCallbacks(
                processingConfig, sequenceGapDetector, smClientRegistry, providerConfigParser, subscriptionRepository,
                vertx, parsedMessageHolder, deadLetterService);
        eventProcessor = new Ed254EventProcessingUseCase(
                processorConfig, parserAdapter, eventExtractor, validator, filterService, persistenceService, callbacks,
                outboxEventConsumer, meterRegistry, interceptorInstances);
    }

    @Test
    void validArrivalEventIsPersistedAndDispatched() {
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-001", VALID_ARRIVAL_SEQUENCE_XML);

        var captor = ArgumentCaptor.forClass(ArrivalEvent.class);
        verify(repository).persist(captor.capture());
        ArrivalEvent saved = captor.getValue();
        assertThat(saved.getSubscriptionId()).isEqualTo("sub-1");
        assertThat(saved.getAerodromeDesignator()).isEqualTo("ESSA");
        assertThat(saved.getMessageType()).isEqualTo("ARRIVAL_SEQUENCE");
        assertThat(saved.isFirstMessageAfterServiceOutage()).isFalse();
        assertThat(saved.getDeliveryStatus()).isEqualTo(OutboxDeliveryStatus.SENT);
        assertThat(saved.getContentHash()).isEqualTo(HashUtil.sha256(VALID_ARRIVAL_SEQUENCE_XML));
        assertThat(saved.getRawPayload()).isEqualTo(VALID_ARRIVAL_SEQUENCE_XML);
        assertThat(saved.getPublicationTime()).isNotNull();
        assertThat(saved.getCreationTime()).isNotNull();

        verify(idempotencyCache).markAsProcessed("sub-1", saved.getContentHash());
        verify(outboxRouterFanOut).route(saved.getMessageId(), VALID_ARRIVAL_SEQUENCE_XML);
    }

    @Test
    void providerExceptionEventIsPersistedCorrectly() {
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-002", VALID_PROVIDER_EXCEPTION_XML);

        var captor = ArgumentCaptor.forClass(ArrivalEvent.class);
        verify(repository).persist(captor.capture());
        assertThat(captor.getValue().getMessageType()).isEqualTo("PROVIDER_EXCEPTION");
    }

    @Test
    void duplicateMessageIsDiscardedWithoutProcessing() {
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(true);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-dup", VALID_ARRIVAL_SEQUENCE_XML);

        verify(repository, never()).persist((ArrivalEvent) any());
        verify(deadLetterService, never()).sendToDeadLetterQueue(
                anyString(), anyString(), anyString(), anyInt(), anyString(), anyString(), any(Exception.class));
        verify(outboxRouterFanOut, never()).route(anyString(), anyString());
    }

    @Test
    void invalidXmlIsSentToDlqAndReportedToProvider() throws Exception {
        String badXml = "<broken>not-ed254</broken>";
        when(jaxbPool.unmarshalAndValidate(badXml))
                .thenThrow(new XmlValidationException("XML validation failed: element 'broken' not expected"));

        ProcessingOutcome outcome = eventProcessor.processAndPersist(
                "sub-1", "queue-1", "msg-bad", badXml);

        assertThat(outcome).isEqualTo(ProcessingOutcome.SKIPPED);
        verify(deadLetterService).sendToDeadLetterQueue(
                eq("sub-1"), eq("queue-1"), eq("msg-bad"), eq(0), eq(badXml),
                eq("VALIDATION_ERROR"), any(Exception.class));
        verify(repository, never()).persist((ArrivalEvent) any());
    }

    @Test
    void extractionFailureIsSentToDlqAndReportedToProvider() throws Exception {
        String noAeroXml = "<arrivalSequence/>";
        when(jaxbPool.unmarshalAndValidate(noAeroXml))
                .thenReturn(buildArrivalMsg(null, null, null, false));

        ProcessingOutcome outcome = eventProcessor.processAndPersist(
                "sub-1", "queue-1", "msg-noaero", noAeroXml);

        assertThat(outcome).isEqualTo(ProcessingOutcome.SKIPPED);
        verify(deadLetterService).sendToDeadLetterQueue(
                eq("sub-1"), eq("queue-1"), eq("msg-noaero"), eq(0), eq(noAeroXml),
                eq("EXTRACTION_ERROR"), any(Exception.class));
        verify(repository, never()).persist((ArrivalEvent) any());
    }

    @Test
    void staleMessageIsSentToDlqAndReportedToProvider() throws Exception {
        Instant staleTime = Instant.now().minus(Duration.ofMinutes(5));
        String staleXml = "<arrivalSequence>stale</arrivalSequence>";
        when(jaxbPool.unmarshalAndValidate(staleXml))
                .thenReturn(buildArrivalMsg("LFPG", staleTime, staleTime, false));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-stale", staleXml);

        verify(deadLetterService).sendToDeadLetterQueue(
                eq("sub-1"), eq("queue-1"), eq("msg-stale"), eq(0), eq(staleXml),
                eq("STALE_MESSAGE"), any(Exception.class));
        verify(smClientRegistry).communicateProblems(eq("sub-1"), any(DataValidationResult.class), any(ProviderConfiguration.class));
        verify(repository, never()).persist((ArrivalEvent) any());
        verify(idempotencyCache, never()).markAsProcessed(anyString(), anyString());
    }

    @Test
    void freshTimestampPassesValidation() throws Exception {
        Instant freshTime = Instant.now().minusSeconds(5);
        String freshXml = "<arrivalSequence>fresh</arrivalSequence>";
        when(jaxbPool.unmarshalAndValidate(freshXml))
                .thenReturn(buildArrivalMsg("EHAM", freshTime, freshTime, false));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-fresh", freshXml);

        verify(repository).persist((ArrivalEvent) any());
        verify(deadLetterService, never()).sendToDeadLetterQueue(
                anyString(), anyString(), anyString(), anyInt(), anyString(), eq("STALE_MESSAGE"), any(Exception.class));
    }

    @Test
    void nullCreationTimePassesTimestampValidation() throws Exception {
        String noTimeXml = "<arrivalSequence>notime</arrivalSequence>";
        when(jaxbPool.unmarshalAndValidate(noTimeXml))
                .thenReturn(buildArrivalMsg("LEMD", Instant.now(), null, false));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-notime", noTimeXml);

        verify(repository).persist((ArrivalEvent) any());
    }

    @Test
    void persistenceFailureIsSentToDlq() {
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);
        doThrow(new RuntimeException("MongoDB connection refused"))
                .when(repository).persist((ArrivalEvent) any());

        assertThatThrownBy(() -> eventProcessor.processAndPersist(
                "sub-1", "queue-1", "msg-fail", VALID_ARRIVAL_SEQUENCE_XML))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("persist");

        verify(deadLetterService).sendToDeadLetterQueue(
                eq("sub-1"), eq("queue-1"), eq("msg-fail"), eq(0), eq(VALID_ARRIVAL_SEQUENCE_XML),
                eq("PERSISTENCE_ERROR"), any(Exception.class));
        verify(idempotencyCache, never()).markAsProcessed(anyString(), anyString());
    }

    @Test
    void contentHashIsMarkedOnlyAfterSuccessfulPersist() {
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-1", VALID_ARRIVAL_SEQUENCE_XML);

        var inOrder = inOrder(repository, idempotencyCache);
        inOrder.verify(repository).persist((ArrivalEvent) any());
        inOrder.verify(idempotencyCache).markAsProcessed(anyString(), anyString());
    }

    @Test
    void communicateProblemsFailureDoesNotBlockProcessing() throws Exception {
        String badXml = "<broken/>";
        when(jaxbPool.unmarshalAndValidate(badXml))
                .thenThrow(new XmlValidationException("validation failed"));

        ProcessingOutcome outcome = eventProcessor.processAndPersist(
                "sub-1", "queue-1", "msg-cp", badXml);

        assertThat(outcome).isEqualTo(ProcessingOutcome.SKIPPED);
        verify(deadLetterService).sendToDeadLetterQueue(
                any(), any(), any(), anyInt(), any(), eq("VALIDATION_ERROR"), any(Exception.class));
    }

    @Test
    void firstMessageAfterServiceOutageIsCaptured() throws Exception {
        String outageXml = "<arrivalSequence>outage</arrivalSequence>";
        when(jaxbPool.unmarshalAndValidate(outageXml))
                .thenReturn(buildArrivalMsg("LFPG", Instant.now(), Instant.now(), true));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-outage", outageXml);

        var captor = ArgumentCaptor.forClass(ArrivalEvent.class);
        verify(repository).persist(captor.capture());
        assertThat(captor.getValue().isFirstMessageAfterServiceOutage()).isTrue();
    }

    // ── EventExtractor ────────────────────────────────────────────────────

    @Test
    void extractorParsesArrivalSequenceMetadata() {
        Ed254Message msg = buildArrivalMsg("ESSA", Instant.parse("2025-01-15T10:30:00Z"),
                Instant.parse("2025-01-15T10:29:55Z"), false);

        var results = eventExtractor.extract(msg);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst()).isPresent();
        ArrivalEvent event = results.getFirst().get();
        assertThat(event.getAerodromeDesignator()).isEqualTo("ESSA");
        assertThat(event.getMessageType()).isEqualTo("ARRIVAL_SEQUENCE");
        assertThat(event.isFirstMessageAfterServiceOutage()).isFalse();
    }

    @Test
    void extractorParsesProviderException() {
        Ed254Message msg = buildExceptionMsg("Test error");

        var results = eventExtractor.extract(msg);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst()).isPresent();
        assertThat(results.getFirst().get().getMessageType()).isEqualTo("PROVIDER_EXCEPTION");
    }

    @Test
    void extractorReturnsEmptyForMissingAerodrome() {
        Ed254Message msg = buildArrivalMsg(null, null, null, false);

        var results = eventExtractor.extract(msg);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst()).isEmpty();
    }

    @Test
    void extractorReturnsEmptyForNull() {
        var results = eventExtractor.extract(null);

        assertThat(results).hasSize(1);
        assertThat(results.getFirst()).isEmpty();
    }

    // ── Subscription Filter ─────────────────────────────────────────────

    @Test
    void filterMismatch_aerodrome_sendsToDeadLetter() {
        filterCache.updateFilters("sub-1", "aerodrome", List.of("ENGM"));

        eventProcessor.processAndPersist("sub-1", "q-1", "msg-filter-aero", VALID_ARRIVAL_SEQUENCE_XML);

        verify(repository, never()).persist(any(ArrivalEvent.class));
        verify(deadLetterService).sendToDeadLetterQueue(
                eq("sub-1"), eq("q-1"), eq("msg-filter-aero"), eq(0), anyString(),
                eq("SUBSCRIPTION_FILTER_MISMATCH"), any(IllegalArgumentException.class));
    }

    @Test
    void filterMatch_aerodrome_persistsNormally() {
        filterCache.updateFilters("sub-1", "aerodrome", List.of("ESSA"));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "q-1", "msg-filter-ok", VALID_ARRIVAL_SEQUENCE_XML);

        verify(repository).persist(any(ArrivalEvent.class));
        verify(deadLetterService, never()).sendToDeadLetterQueue(
                any(), anyString(), anyString(), anyInt(), anyString(),
                eq("SUBSCRIPTION_FILTER_MISMATCH"), any());
    }

    @Test
    void emptyFilterCache_allowsAll() {
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "q-1", "msg-filter-empty", VALID_ARRIVAL_SEQUENCE_XML);

        verify(repository).persist(any(ArrivalEvent.class));
    }

    // ── Metrics ───────────────────────────────────────────────────────────

    @Test
    void processingTimerIsRecordedForEachMessageType() {
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "queue-1", "msg-t1", VALID_ARRIVAL_SEQUENCE_XML);

        assertThat(meterRegistry.find("ed254_event_processing_seconds").timer()).isNotNull();
        assertThat(meterRegistry.find("ed254_event_processing_seconds").timer().count()).isEqualTo(1);
    }

    @Test
    void desiredSubscriptionConfigHashIsStableAcrossCalls() {
        DesiredSubscription subscription = new DesiredSubscription(
                "validator",
                List.of(new DestinationAerodrome("LPPT", null)),
                null, null,
                new SupplementaryData(true, false, false, false, false),
                "Test");

        assertThat(subscription.generateConfigHash()).isEqualTo(subscription.generateConfigHash());
    }

    @Test
    void desiredSubscriptionMapsToSubscriptionRequest() {
        DesiredSubscription subscription = new DesiredSubscription(
                "validator",
                List.of(new DestinationAerodrome("LPPT", null), new DestinationAerodrome("ESSA", null)),
                List.of("AMRAM"), null,
                new SupplementaryData(true, true, false, false, false),
                "Multi");

        var filters = new com.github.swim_developer.ed254.consumer.domain.model.SubscriptionFilters(
                subscription.destinationAerodrome(), subscription.pointName(), subscription.flightSelector());
        SubscriptionRequest request = new SubscriptionRequest(filters, subscription.supplementaryData());

        assertThat(request.subscriptionFilters()).isNotNull();
        assertThat(request.subscriptionFilters().destinationAerodrome()).hasSize(2);
        assertThat(request.subscriptionFilters().pointName()).containsExactly("AMRAM");
        assertThat(request.supplementaryData().delay()).isTrue();
        assertThat(request.supplementaryData().landingSequencePosition()).isTrue();
    }

    // ── Sequence Gap Detection Integration ────────────────────────────────

    @Test
    void sequenceGapDetected_reportsToProviderButStillPersists() {
        DataValidationResult gapResult = DataValidationResult.sequenceGaps(List.of("TAP1234"));
        when(sequenceGapDetector.detect(eq("q-1"), any(ArrivalSequence.class))).thenReturn(Optional.of(gapResult));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "q-1", "msg-gap", VALID_ARRIVAL_SEQUENCE_XML);

        verify(smClientRegistry).communicateProblems(eq("sub-1"), eq(gapResult), any(ProviderConfiguration.class));
        verify(repository).persist(any(ArrivalEvent.class));
    }

    @Test
    void noSequenceGap_doesNotReportToProvider() {
        when(sequenceGapDetector.detect(anyString(), any(ArrivalSequence.class))).thenReturn(Optional.empty());
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "q-1", "msg-nogap", VALID_ARRIVAL_SEQUENCE_XML);

        verify(providerClient, never()).communicateProblems(any());
        verify(repository).persist(any(ArrivalEvent.class));
    }

    // ── SupplementaryData / SequenceEntries ─────────────────────────────

    @Test
    void extractorCapturesSequenceEntriesCount() {
        ArrivalSequence seq = new ArrivalSequence();
        seq.setAerodromeDesignator("LPPT");
        seq.setPublicationTime(toXmlCal(Instant.now()));
        seq.setCreationTime(toXmlCal(Instant.now()));
        seq.setFirstMessageAfterServiceOutage(false);
        ArrivalSequence.SequenceEntries entries = new ArrivalSequence.SequenceEntries();
        entries.getArrivalManagementInformation().add(new ArrivalSequence.SequenceEntries.ArrivalManagementInformation());
        entries.getArrivalManagementInformation().add(new ArrivalSequence.SequenceEntries.ArrivalManagementInformation());
        entries.getArrivalManagementInformation().add(new ArrivalSequence.SequenceEntries.ArrivalManagementInformation());
        seq.setSequenceEntries(entries);

        var results = eventExtractor.extract(new Ed254Message.ArrivalMsg(seq));

        assertThat(results).hasSize(1);
        assertThat(results.getFirst()).isPresent();
        assertThat(results.getFirst().get().getSequenceEntriesCount()).isEqualTo(3);
    }

    // ── Anti-Jitter (Sequential Staleness) ───────────────────────────────

    @Test
    void twoFreshMessagesInOrder_bothPersisted() throws Exception {
        Instant t1 = Instant.now().minusSeconds(5);
        Instant t2 = Instant.now().minusSeconds(2);
        String xml1 = "<arrivalSequence>t1</arrivalSequence>";
        String xml2 = "<arrivalSequence>t2</arrivalSequence>";
        when(jaxbPool.unmarshalAndValidate(xml1)).thenReturn(buildArrivalMsg("EGLL", t1, t1, false));
        when(jaxbPool.unmarshalAndValidate(xml2)).thenReturn(buildArrivalMsg("EGLL", t2, t2, false));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        eventProcessor.processAndPersist("sub-1", "q-1", "msg-t1", xml1);
        eventProcessor.processAndPersist("sub-1", "q-1", "msg-t2", xml2);

        verify(repository, times(2)).persist(any(ArrivalEvent.class));
    }

    @Test
    void staleMessageAfterFreshOne_rejectedByTimestampValidation() throws Exception {
        Instant stale = Instant.now().minus(Duration.ofMinutes(10));
        String staleXml = "<arrivalSequence>stale-after-fresh</arrivalSequence>";
        when(jaxbPool.unmarshalAndValidate(staleXml)).thenReturn(buildArrivalMsg("EDDF", stale, stale, false));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);

        ProcessingOutcome outcome = eventProcessor.processAndPersist("sub-1", "q-1", "msg-stale2", staleXml);

        assertThat(outcome).isEqualTo(ProcessingOutcome.SKIPPED);
        verify(deadLetterService).sendToDeadLetterQueue(
                eq("sub-1"), eq("q-1"), eq("msg-stale2"), eq(0), eq(staleXml),
                eq("STALE_MESSAGE"), any(Exception.class));
        verify(repository, never()).persist(any(ArrivalEvent.class));
    }

    // ── First Message After Outage + Gap Detection ───────────────────────

    @Test
    void firstMessageAfterOutage_triggersGapDetectionAndPersists() throws Exception {
        String outageXml = "<arrivalSequence>outage-gap</arrivalSequence>";
        DataValidationResult gapResult = DataValidationResult.sequenceGaps(List.of("TAP1234", "SAS5678"));
        when(jaxbPool.unmarshalAndValidate(outageXml))
                .thenReturn(buildArrivalMsg("LFPG", Instant.now(), Instant.now(), true));
        when(idempotencyCache.isAlreadyProcessed(anyString(), anyString())).thenReturn(false);
        when(sequenceGapDetector.detect(eq("q-1"), any(ArrivalSequence.class))).thenReturn(Optional.of(gapResult));

        eventProcessor.processAndPersist("sub-1", "q-1", "msg-outage-gap", outageXml);

        var captor = ArgumentCaptor.forClass(ArrivalEvent.class);
        verify(repository).persist(captor.capture());
        assertThat(captor.getValue().isFirstMessageAfterServiceOutage()).isTrue();
        verify(smClientRegistry).communicateProblems(eq("sub-1"), eq(gapResult), any(ProviderConfiguration.class));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static Ed254Message buildArrivalMsg(String aerodrome, Instant pubTime, Instant createTime, boolean outage) {
        ArrivalSequence seq = new ArrivalSequence();
        seq.setAerodromeDesignator(aerodrome);
        seq.setPublicationTime(toXmlCal(pubTime));
        seq.setCreationTime(toXmlCal(createTime));
        seq.setFirstMessageAfterServiceOutage(outage);
        return new Ed254Message.ArrivalMsg(seq);
    }

    private static Ed254Message buildExceptionMsg(String message) {
        ProviderExceptions pe = new ProviderExceptions();
        pe.setProvException(message);
        return new Ed254Message.ExceptionMsg(pe);
    }

    private static XMLGregorianCalendar toXmlCal(Instant instant) {
        if (instant == null) return null;
        try {
            return DatatypeFactory.newInstance()
                    .newXMLGregorianCalendar(GregorianCalendar.from(
                            ZonedDateTime.ofInstant(instant, java.time.ZoneOffset.UTC)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
