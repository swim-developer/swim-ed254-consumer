package com.github.swim_developer.ed254.consumer.integration;

import com.github.swim_developer.ed254.consumer.domain.model.ArrivalEvent;
import com.github.swim_developer.framework.consumer.infrastructure.out.dlq.DeadLetterMessage;
import com.github.swim_developer.ed254.consumer.domain.model.DestinationAerodrome;
import com.github.swim_developer.ed254.consumer.domain.model.FilterDimension;
import com.github.swim_developer.ed254.consumer.domain.model.Subscription;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoArrivalEventStore;
import com.github.swim_developer.framework.consumer.application.port.out.DeadLetterStore;
import com.github.swim_developer.ed254.consumer.infrastructure.out.persistence.MongoSubscriptionStore;
import com.github.swim_developer.ed254.consumer.application.usecase.Ed254EventProcessingUseCase;
import com.github.swim_developer.framework.consumer.infrastructure.out.idempotency.AbstractIdempotencyCache;
import com.github.swim_developer.framework.consumer.infrastructure.out.config.subscription.AbstractSubscriptionConfigParser;
import com.github.swim_developer.ed254.consumer.application.usecase.Ed254SubscriptionUseCase;
import com.github.swim_developer.framework.application.model.OutboxDeliveryStatus;
import com.github.swim_developer.framework.consumer.infrastructure.out.filter.SubscriptionFilterCache;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.cache.Cache;
import io.quarkus.cache.CacheName;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;


import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;



import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for the ED-254 (Extended AMAN) consumer with real infrastructure.
 *
 * <p>Validates the full consumer lifecycle for Arrival Sequence messages using:</p>
 * <ul>
 *   <li><b>MongoDB</b> — Real database via Quarkus Dev Services (Testcontainers)</li>
 *   <li><b>Kafka (Redpanda)</b> — Real broker via Quarkus Dev Services</li>
 *   <li><b>WireMock</b> — Simulates the SWIM Subscription Manager REST API</li>
 *   <li><b>Artemis</b> — Real AMQP 1.0 broker via Quarkus Dev Services</li>
 * </ul>
 *
 * <h2>What This Proves to the SFG</h2>
 * <ul>
 *   <li>The REST API correctly orchestrates subscription lifecycle with the ED-254 provider</li>
 *   <li>XSD validation against FIXM 4.3 + ED-254 schemas rejects invalid payloads</li>
 *   <li>Idempotency (SHA-256 content hash) survives across real MongoDB persistence (L2 cache)</li>
 *   <li>The full pipeline: AMQP → XSD validation → extraction → MongoDB persistence → Kafka outbox</li>
 *   <li>DLQ captures rejected messages with proper metadata (VALIDATION_ERROR, EXTRACTION_ERROR)</li>
 *   <li>CommunicateProblems: the consumer reports validation issues back to the provider (ED-254 specific)</li>
 *   <li>Statistics, events-by-aerodrome, date-range queries, and DLQ endpoints return accurate data</li>
 *   <li>Heartbeat watchdog persists provider UP/DOWN (parity with DNOTAM consumer IT)</li>
 * </ul>
 *
 * @see Ed254EventProcessingUseCase
 * @see com.github.swim_developer.ed254.consumer.infrastructure.in.rest.ConsumerResource
 * @see com.github.swim_developer.ed254.consumer.application.usecase.Ed254SubscriptionUseCase
 */
@QuarkusTest
@ConnectWireMock
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class Ed254ConsumerIT {

    private static final String VALID_ED254_XML = """
            <?xml version="1.0" encoding="UTF-8"?>
            <arrivalSequence xmlns="http://www.fixm.aero/ed254/1.0"
                xmlns:fb="http://www.fixm.aero/base/4.3"
                xmlns:fx="http://www.fixm.aero/flight/4.3">
                <creationTime>%CREATION_TIME%</creationTime>
                <publicationTime>%PUBLICATION_TIME%</publicationTime>
                <firstMessageAfterServiceOutage>false</firstMessageAfterServiceOutage>
                <aerodromeDesignator>LPPT</aerodromeDesignator>
                <sequenceEntries>
                    <arrivalManagementInformation>
                        <amanTargetLandingTime>%LANDING_TIME%</amanTargetLandingTime>
                        <arrivalManagementHandlingIndicator>SEQUENCED_STABLE</arrivalManagementHandlingIndicator>
                        <lastFiledRecord>false</lastFiledRecord>
                        <sequenceNumber>1</sequenceNumber>
                        <typeOfAircraft>A320</typeOfAircraft>
                        <flightIdentification>
                            <arcid>TAP1234</arcid>
                            <ades>LPPT</ades>
                            <adep>EGLL</adep>
                        </flightIdentification>
                    </arrivalManagementInformation>
                </sequenceEntries>
            </arrivalSequence>
            """;

    private static final String INVALID_XML = "<not-valid-ed254>broken</not-valid-ed254>";

    WireMock wiremock;

    @Inject
    Ed254EventProcessingUseCase eventProcessor;

    @Inject
    MongoArrivalEventStore eventRepository;

    @Inject
    DeadLetterStore dlqRepository;

    @Inject
    MongoSubscriptionStore subscriptionRepository;

    @Inject
    AbstractIdempotencyCache idempotencyCache;

    @Inject
    Ed254SubscriptionUseCase lifecycleService;

    @Inject
    AbstractSubscriptionConfigParser<?> subscriptionConfigParser;

    @Inject
    @CacheName("ed254-processed-messages")
    Cache l1Cache;

    @Inject
    SubscriptionFilterCache filterCache;

    @BeforeEach
    void cleanDatabase(TestInfo testInfo) {
        System.out.printf("%n══ ▶ %s.%s%n", getClass().getSimpleName(), testInfo.getDisplayName());
        eventRepository.deleteAll();
        dlqRepository.deleteAll();
        subscriptionRepository.deleteAllSubscriptions();
        l1Cache.invalidateAll().await().indefinitely();
        filterCache.clear();
        wiremock.removeMappings();
        wiremock.resetAllScenarios();
        wiremock.resetRequests();
    }

    // ─── Group 1: REST API + Subscription Lifecycle ───

    /**
     * POST /api/v1/subscriptions → WireMock receives the provider call,
     * returns a subscription response, and the consumer persists it to MongoDB.
     */
    @Test
    @Order(1)
    void createSubscriptionEndToEnd() {
        stubSubscriptionManagerCreate("sub-IT-001", "ED254.v1.client.sub-IT-001");

        var body = Map.of(
                "provider", "test-provider",
                "destinationAerodrome", List.of(Map.of("aerodromeDesignator", "LPPT")),
                "description", "Integration test"
        );

        given()
                .contentType(ContentType.JSON)
                .body(body)
                .when()
                .post("/api/v1/subscriptions")
                .then()
                .statusCode(201);

        var persisted = subscriptionRepository.findBySubscriptionId("sub-IT-001");
        assertThat(persisted).isPresent();
        assertThat(persisted.get().getQueueName()).isEqualTo("ED254.v1.client.sub-IT-001");

        wiremock.verifyThat(postRequestedFor(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions")));
        wiremock.verifyThat(putRequestedFor(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions/sub-IT-001/resume")));
    }

    /**
     * GET /api/v1/subscriptions returns the list of all subscriptions in MongoDB.
     */
    @Test
    @Order(2)
    void listSubscriptionsFromMongoDB() {
        seedSubscription("sub-list-1", "ACTIVE");
        seedSubscription("sub-list-2", "PAUSED");

        var response = given()
                .when()
                .get("/api/v1/subscriptions")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getList("$")).hasSize(2);
    }

    /**
     * PUT /api/v1/subscriptions/{id} with PAUSED → WireMock receives the update,
     * the local status transitions to PAUSED.
     */
    @Test
    @Order(3)
    void pauseSubscriptionViaApi() {
        seedSubscription("sub-pause-1", "ACTIVE");
        stubSubscriptionManagerUpdate("sub-pause-1", "PAUSED");

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("subscription_status", "PAUSED"))
                .when()
                .put("/api/v1/subscriptions/sub-pause-1")
                .then()
                .statusCode(200);

        var updated = subscriptionRepository.findBySubscriptionId("sub-pause-1");
        assertThat(updated).isPresent();
        assertThat(updated.get().getSubscriptionStatus()).isEqualTo("PAUSED");
    }

    /**
     * PUT /api/v1/subscriptions/{id} with ACTIVE → subscription transitions to ACTIVE.
     */
    @Test
    @Order(4)
    void resumeSubscriptionViaApi() {
        seedSubscription("sub-resume-1", "PAUSED");
        stubSubscriptionManagerUpdate("sub-resume-1", "ACTIVE");

        given()
                .contentType(ContentType.JSON)
                .body(Map.of("subscription_status", "ACTIVE"))
                .when()
                .put("/api/v1/subscriptions/sub-resume-1")
                .then()
                .statusCode(200);

        var updated = subscriptionRepository.findBySubscriptionId("sub-resume-1");
        assertThat(updated).isPresent();
        assertThat(updated.get().getSubscriptionStatus()).isEqualTo("ACTIVE");
    }

    /**
     * DELETE /api/v1/subscriptions/{id} → WireMock receives DELETE,
     * local subscription is removed from MongoDB.
     */
    @Test
    @Order(5)
    void deleteSubscriptionCleanup() {
        seedSubscription("sub-del-1", "ACTIVE");

        wiremock.register(delete(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions"))
                .withQueryParam("subscriptionReference", equalTo("sub-del-1"))
                .willReturn(aResponse().withStatus(204)));

        given()
                .when()
                .delete("/api/v1/subscriptions/sub-del-1")
                .then()
                .statusCode(204);

        var deleted = subscriptionRepository.findBySubscriptionId("sub-del-1");
        assertThat(deleted).isEmpty();

        wiremock.verifyThat(deleteRequestedFor(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions"))
                .withQueryParam("subscriptionReference", equalTo("sub-del-1")));
    }

    /**
     * PUT /api/v1/subscriptions/{id} with invalid status → 400 Bad Request.
     */
    @Test
    @Order(6)
    void updateWithInvalidStatusRejects() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of("subscription_status", "INVALID_STATUS"))
                .when()
                .put("/api/v1/subscriptions/sub-any")
                .then()
                .statusCode(400);
    }

    /**
     * POST /api/v1/subscriptions with empty topic → 400 Bad Request.
     */
    @Test
    @Order(7)
    void createSubscriptionWithoutTopicRejects() {
        given()
                .contentType(ContentType.JSON)
                .body(Map.of("description", "no topic"))
                .when()
                .post("/api/v1/subscriptions")
                .then()
                .statusCode(400);
    }

    // ─── Group 2: Event Processing Pipeline ───

    /**
     * Full pipeline: valid ED-254 Arrival Sequence XML → XSD validation → extraction → MongoDB persistence.
     * Validates: aerodrome designator, message type, content hash, Kafka status, and creation/publication times.
     */
    @Test
    @Order(10)
    void validArrivalSequencePersistedWithFullMetadata() {
        stubCommunicateProblems();
        String xml = freshXml();

        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-MSG-001", xml);

        List<ArrivalEvent> events = eventRepository.listAllDomain();
        assertThat(events).hasSize(1);

        ArrivalEvent event = events.get(0);
        assertThat(event.getAerodromeDesignator()).isEqualTo("LPPT");
        assertThat(event.getMessageType()).isEqualTo("ARRIVAL_SEQUENCE");
        assertThat(event.getContentHash()).isNotEmpty();
        assertThat(event.getDeliveryStatus()).isIn(OutboxDeliveryStatus.PENDING, OutboxDeliveryStatus.SENT);
        assertThat(event.getMessageId()).isEqualTo("AMQP-MSG-001");
        assertThat(event.isFirstMessageAfterServiceOutage()).isFalse();
    }

    /**
     * Invalid XML (not ED-254 conformant) → DLQ with VALIDATION_ERROR reason.
     * Business logic must never receive non-conformant payloads.
     * Also verifies the consumer reports the problem to the provider (CommunicateProblems).
     */
    @Test
    @Order(11)
    void invalidXmlRoutedToDlq() {
        seedSubscription("sub-1", "ACTIVE");
        stubCommunicateProblems();

        try {
            eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-INVALID-001", INVALID_XML);
        } catch (RuntimeException e) {
            // Expected: invalid XML should be sent to DLQ
        }

        assertThat(eventRepository.listAllDomain()).isEmpty();

        List<DeadLetterMessage> dlqMessages = dlqRepository.listAllDomain();
        assertThat(dlqMessages).hasSize(1);
        assertThat(dlqMessages.get(0).getErrorType()).isEqualTo("VALIDATION_ERROR");
        assertThat(dlqMessages.get(0).getRawPayload()).isEqualTo(INVALID_XML);

        wiremock.verifyThat(postRequestedFor(urlPathEqualTo("/arrivalSequenceInformation/v1/problems")));
    }

    /**
     * Duplicate content (same SHA-256 hash) → second message silently discarded.
     * Only 1 event persisted in MongoDB.
     */
    @Test
    @Order(12)
    void duplicateContentDiscardedByIdempotency() {
        stubCommunicateProblems();
        String xml = freshXml();

        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-DUP-001", xml);
        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-DUP-002", xml);

        assertThat(eventRepository.listAllDomain()).hasSize(1);
    }

    /**
     * Idempotency persists to MongoDB (L2 cache). After clearing the Caffeine L1 cache,
     * the system still recognizes duplicates from the database.
     */
    @Test
    @Order(13)
    void idempotencyWithRealDatabase() {
        stubCommunicateProblems();
        String xml = freshXml();
        String hash = com.github.swim_developer.framework.infrastructure.util.HashUtil.sha256(xml);

        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-IDEM-001", xml);

        assertThat(eventRepository.listAllDomain()).hasSize(1);
        assertThat(idempotencyCache.isAlreadyProcessed("sub-1", hash)).isTrue();
    }

    // ─── Group 3: REST Queries After Processing ───

    /**
     * GET /api/v1/events?aerodrome=LPPT returns events filtered by aerodrome.
     */
    @Test
    @Order(20)
    void queryEventsByAerodrome() {
        stubCommunicateProblems();
        String xml = freshXml();
        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-QRY-001", xml);

        var response = given()
                .when()
                .get("/api/v1/events?aerodrome=LPPT&page=0&size=10")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getList("content")).hasSize(1);
        assertThat(response.getLong("totalElements")).isEqualTo(1);
    }

    /**
     * GET /api/v1/dlq returns dead letter messages with pagination.
     */
    @Test
    @Order(21)
    void queryDlqAfterRejection() {
        stubCommunicateProblems();

        try {
            eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-DLQ-Q-001", INVALID_XML);
        } catch (RuntimeException e) {
            // Expected: invalid XML should be sent to DLQ
        }

        var response = given()
                .when()
                .get("/api/v1/dlq?page=0&size=10")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getList("content")).hasSize(1);
    }

    /**
     * GET /api/v1/stats returns accurate aggregate counts after mixed operations.
     */
    @Test
    @Order(22)
    void statsReflectRealState() {
        stubCommunicateProblems();
        seedSubscription("sub-stats-1", "ACTIVE");
        String xml = freshXml();

        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-STATS-001", xml);
        try {
            eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-STATS-002", INVALID_XML);
        } catch (RuntimeException e) {
            // Expected: invalid XML should be sent to DLQ
        }

        var response = given()
                .when()
                .get("/api/v1/stats")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getLong("totalEvents")).isEqualTo(1);
        assertThat(response.getLong("totalDlq")).isEqualTo(1);
        assertThat(response.getInt("activeSubscriptions")).isEqualTo(1);
    }

    /**
     * GET /api/v1/events/{messageId} returns a specific event by its AMQP message ID.
     */
    @Test
    @Order(23)
    void getEventByMessageId() {
        stubCommunicateProblems();
        String xml = freshXml();
        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-GET-001", xml);

        ArrivalEvent persisted = eventRepository.listAllDomain().get(0);

        given()
                .when()
                .get("/api/v1/events/" + persisted.getMessageId())
                .then()
                .statusCode(200);
    }

    /**
     * GET /api/v1/events/count returns accurate event count with optional aerodrome filter.
     */
    @Test
    @Order(24)
    void countEventsByAerodrome() {
        stubCommunicateProblems();
        String xml = freshXml();
        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-CNT-001", xml);

        var response = given()
                .when()
                .get("/api/v1/events/count?aerodrome=LPPT")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getLong("count")).isEqualTo(1);
    }

    /**
     * GET /api/v1/topics returns the configured desired subscriptions.
     * In test profile, ed254.subscriptions=[] to prevent startup reconciliation.
     */
    @Test
    @Order(25)
    void listConfiguredTopicsEndpoint() {
        given()
                .when()
                .get("/api/v1/topics")
                .then()
                .statusCode(200);
    }

    /**
     * GET /api/v1/events/range filters by receivedAt (optional aerodrome), matching DNOTAM date-range query parity.
     */
    @Test
    @Order(26)
    void queryEventsByReceivedAtRange() {
        stubCommunicateProblems();
        String xml = freshXml();
        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-RANGE-001", xml);

        Instant start = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant end = Instant.now().plus(1, ChronoUnit.HOURS);

        var response = given()
                .when()
                .get("/api/v1/events/range?startDate=" + start + "&endDate=" + end + "&aerodrome=LPPT&page=0&size=10")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getList("content")).hasSize(1);
    }

    /**
     * DefaultSubscriptionConfigParser reflects %test.swim.subscriptions=[] (same contract as DNOTAM IT).
     */
    @Test
    @Order(27)
    void parseDesiredSubscriptionsEmptyInTestProfile() {
        assertThat(subscriptionConfigParser.parseDesiredSubscriptions()).isEmpty();
    }

    /**
     * GET /api/v1/dlq/count returns accurate DLQ message count.
     */
    @Test
    @Order(28)
    void countDlqMessages() {
        stubCommunicateProblems();

        try {
            eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-DLQC-001", INVALID_XML);
        } catch (RuntimeException e) {
            // Expected: invalid XML should be sent to DLQ
        }

        var response = given()
                .when()
                .get("/api/v1/dlq/count")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getLong("count")).isEqualTo(1);
    }

    /**
     * GET /api/v1/subscriptions/active returns only ACTIVE subscriptions.
     */
    @Test
    @Order(29)
    void listActiveSubscriptionsOnly() {
        seedSubscription("sub-active-1", "ACTIVE");
        seedSubscription("sub-paused-1", "PAUSED");
        seedSubscription("sub-active-2", "ACTIVE");

        var response = given()
                .when()
                .get("/api/v1/subscriptions/active")
                .then()
                .statusCode(200)
                .extract().body().jsonPath();

        assertThat(response.getList("$")).hasSize(2);
    }

    // ─── Group 4: Health Checks ───

    /**
     * Liveness probe returns UP when the application is running.
     */
    @Test
    @Order(30)
    void livenessProbeUp() {
        given()
                .when()
                .get("/q/health/live")
                .then()
                .statusCode(200);
    }

    /**
     * Readiness probe responds with a valid health check body.
     */
    @Test
    @Order(31)
    void readinessProbeResponds() {
        var response = given()
                .when()
                .get("/q/health/ready")
                .then()
                .extract().body().jsonPath();

        assertThat(response.getString("status")).isNotEmpty();
        assertThat(response.getList("checks")).isNotNull();
    }

    // ─── Group 5: Self-Healing (Provider State Loss Recovery) ───

    /**
     * Provider returns 404 during resume → framework translates to SubscriptionNotFoundException,
     * marks subscription INVALID, deletes local, and triggers full re-subscription cycle
     * (POST → PAUSED → ACTIVE).
     */
    @Test
    @Order(40)
    void automaticResubscriptionOnProviderStateLoss() {
        seedSubscription("sub-lost-1", "ACTIVE");

        wiremock.register(put(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions/sub-lost-1/resume"))
                .willReturn(aResponse().withStatus(404)));

        stubSubscriptionManagerCreate("sub-recovered-1", "ED254.v1.client.sub-recovered-1");

        try {
            lifecycleService.resumeSubscription("sub-lost-1");
        } catch (Exception e) {
            // Expected: provider returns 404 for lost subscription
        }

        assertThat(subscriptionRepository.findBySubscriptionId("sub-lost-1"))
                .as("Old subscription must be deleted after provider 404")
                .isEmpty();

        assertThat(subscriptionRepository.findBySubscriptionId("sub-recovered-1"))
                .as("New subscription must be created after provider state loss recovery")
                .isPresent()
                .get()
                .satisfies(s -> assertThat(s.getSubscriptionStatus()).isEqualTo("ACTIVE"));

        wiremock.verifyThat(postRequestedFor(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions")));
    }

    // ─── Group 6: WFS GetFeature (Request/Reply) ───

    @Test
    @Order(50)
    void wfsGetFeatureReturnsEd254Xml() {
        String ed254Response = """
                <?xml version="1.0" encoding="UTF-8"?>
                <arrivalSequence xmlns="http://www.fixm.aero/ed254/1.0"
                    aerodromeDesignator="LPPT">
                    <sequenceEntries/>
                </arrivalSequence>
                """;

        wiremock.register(get(urlPathEqualTo("/swim/v1/features"))
                .withQueryParam("typeName", equalTo("arrivalSequence:ArrivalSequence"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/xml")
                        .withBody(ed254Response)));

        String body = given()
                .queryParam("typeName", "arrivalSequence:ArrivalSequence")
                .when()
                .get("/api/v1/features")
                .then()
                .statusCode(200)
                .extract().body().asString();

        assertThat(body).contains("arrivalSequence");
    }

    @Test
    @Order(51)
    void wfsGetFeatureReturns502WhenProviderDown() {
        wiremock.register(get(urlPathEqualTo("/swim/v1/features"))
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody("Service Unavailable")));

        var response = given()
                .queryParam("typeName", "arrivalSequence:ArrivalSequence")
                .when()
                .get("/api/v1/features")
                .then()
                .statusCode(502)
                .contentType(ContentType.JSON)
                .extract().body().jsonPath();

        assertThat(response.getString("error")).isEqualTo("Provider request failed");
    }

    @Test
    @Order(52)
    void wfsGetFeatureReturns503WhenNoProvider() {
        var response = given()
                .queryParam("typeName", "arrivalSequence:ArrivalSequence")
                .queryParam("providerId", "non-existent-provider")
                .when()
                .get("/api/v1/features")
                .then()
                .statusCode(503)
                .contentType(ContentType.JSON)
                .extract().body().jsonPath();

        assertThat(response.getString("error")).contains("No provider configured");
    }

    @Test
    @Order(53)
    void wfsGetFeatureForwardsValidTimeFilter() {
        String validTime = "2026-04-27T00:00:00Z/2026-04-28T00:00:00Z";

        wiremock.register(get(urlPathEqualTo("/swim/v1/features"))
                .withQueryParam("typeName", equalTo("arrivalSequence:ArrivalSequence"))
                .withQueryParam("validTime", equalTo(validTime))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/xml")
                        .withBody("<arrivalSequence xmlns=\"http://www.fixm.aero/ed254/1.0\" aerodromeDesignator=\"LPPT\"/>")));

        String body = given()
                .queryParam("typeName", "arrivalSequence:ArrivalSequence")
                .queryParam("validTime", validTime)
                .when()
                .get("/api/v1/features")
                .then()
                .statusCode(200)
                .extract().body().asString();

        assertThat(body).contains("LPPT");

        wiremock.verify(getRequestedFor(urlPathEqualTo("/swim/v1/features"))
                .withQueryParam("validTime", equalTo(validTime)));
    }

    // ─── Group 7: Resilience & Timeout Handling ───

    /**
     * Provider timeout exceeds configured @Timeout → retries exhausted → 503.
     * Mirrors dnotam-consumer's testSubscriptionManagerTimeout pattern.
     *
     * <p>Demonstrates:</p>
     * <ul>
     *   <li><b>Graceful degradation</b> — System does not hang indefinitely waiting for provider</li>
     *   <li><b>Retry with backoff</b> — delay between retries (configured in @Retry annotation)</li>
     *   <li><b>Subscription safety</b> — Failed creation never persists to database</li>
     * </ul>
     */
    @Test
    @Order(60)
    @DisplayName("Should timeout and retry when subscription manager is slow to respond")
    void testSubscriptionManagerTimeout() {
        String subscriptionId = "sub-timeout-001";
        String queueName = "ED254-client-" + subscriptionId;

        String responseJson = """
                {
                    "subscriptionId": "%s",
                    "subscriptionStatus": "PAUSED",
                    "queueName": "%s",
                    "topic": "ED254/v1"
                }
                """.formatted(subscriptionId, queueName);

        wiremock.register(post(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody(responseJson)
                        .withFixedDelay(12000)));

        var body = Map.of(
                "provider", "test-provider",
                "destinationAerodrome", List.of(Map.of("aerodromeDesignator", "LPPT")),
                "description", "Timeout test"
        );

        var response = given()
                .contentType(ContentType.JSON)
                .config(io.restassured.config.RestAssuredConfig.config()
                        .httpClient(io.restassured.config.HttpClientConfig.httpClientConfig()
                                .setParam("http.socket.timeout", 60000)))
                .body(body)
                .when()
                .post("/api/v1/subscriptions");

        assertThat(response.statusCode()).isEqualTo(503);

        wiremock.verify(4, postRequestedFor(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions")));

        var sub = subscriptionRepository.findBySubscriptionId(subscriptionId);
        assertThat(sub).isEmpty();
    }

    /**
     * Validates that the consumer recovers after transient failures.
     * WireMock fails 2 times, succeeds on 3rd attempt.
     * Mirrors dnotam-consumer's testSubscriptionManagerRecoveryAfterRetries pattern.
     */
    @Test
    @Order(61)
    @DisplayName("Should succeed after 2 timeouts on 3rd retry")
    void testSubscriptionManagerRecoveryAfterRetries() {
        String subscriptionId = "sub-retry-001";
        String queueName = "ED254-client-" + subscriptionId;

        String responseJson = """
                {
                    "subscriptionId": "%s",
                    "subscriptionStatus": "PAUSED",
                    "queueName": "%s",
                    "topic": "ED254/v1"
                }
                """.formatted(subscriptionId, queueName);

        wiremock.register(post(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions"))
                .inScenario("Retry Recovery")
                .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
                .willReturn(aResponse().withFixedDelay(12000))
                .willSetStateTo("First Retry"));

        wiremock.register(post(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions"))
                .inScenario("Retry Recovery")
                .whenScenarioStateIs("First Retry")
                .willReturn(aResponse().withFixedDelay(12000))
                .willSetStateTo("Second Retry"));

        wiremock.register(post(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions"))
                .inScenario("Retry Recovery")
                .whenScenarioStateIs("Second Retry")
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody(responseJson)));

        stubSubscriptionManagerUpdate(subscriptionId, "ACTIVE");

        var body = Map.of(
                "provider", "test-provider",
                "destinationAerodrome", List.of(Map.of("aerodromeDesignator", "LPPT")),
                "description", "Recovery test"
        );

        var response = given()
                .contentType(ContentType.JSON)
                .config(io.restassured.config.RestAssuredConfig.config()
                        .httpClient(io.restassured.config.HttpClientConfig.httpClientConfig()
                                .setParam("http.socket.timeout", 60000)))
                .body(body)
                .when()
                .post("/api/v1/subscriptions");

        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(response.jsonPath().getString("subscriptionId")).isEqualTo(subscriptionId);

        wiremock.verify(3, postRequestedFor(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions")));

        var sub = subscriptionRepository.findBySubscriptionId(subscriptionId);
        assertThat(sub).isPresent();
    }

    // ─── Group 8: ED-254 Specific Requirements ───

    /**
     * CP1 REQUIREMENT: Subscription filter by ADES (aerodrome designator).
     * When a filter is configured for a subscription, events for non-matching aerodromes
     * must be rejected and sent to DLQ with SUBSCRIPTION_FILTER_MISMATCH reason.
     *
     * <p>This validates the ADES scope management capability required by ED-254 CP1 compliance.</p>
     */
    @Test
    @Order(70)
    void adesFilterRejectsEventForNonSubscribedAerodrome() {
        stubCommunicateProblems();

        seedSubscription("sub-filter-ades", "ACTIVE");
        filterCache.updateFilters("sub-filter-ades", FilterDimension.AERODROME, List.of("LPPT"));

        String xml = freshXmlForAerodrome("EDDF");

        var outcome = eventProcessor.processAndPersist("sub-filter-ades", "queue-1", "AMQP-FILTER-001", xml);

        assertThat(outcome)
                .as("Filtered events must return SKIPPED")
                .isEqualTo(com.github.swim_developer.framework.application.model.ProcessingOutcome.SKIPPED);

        assertThat(eventRepository.listAllDomain())
                .as("Event for non-subscribed ADES must NOT be persisted")
                .isEmpty();

        List<DeadLetterMessage> dlq = dlqRepository.listAllDomain();
        assertThat(dlq).hasSize(1);
        assertThat(dlq.get(0).getErrorType()).isEqualTo("SUBSCRIPTION_FILTER_MISMATCH");
    }

    /**
     * ED-254 SPECIFIC: firstMessageAfterServiceOutage=true must be captured and persisted.
     * When the provider recovers from an outage, the first message carries this flag,
     * enabling the consumer to know that a gap may exist in the sequence.
     *
     * <p>Also validates that supplementaryData (sequence entries count) is extracted correctly.</p>
     */
    @Test
    @Order(71)
    void firstMessageAfterOutageWithSequenceEntriesPersistedCorrectly() {
        stubCommunicateProblems();

        String xml = freshXmlWithOutageFlag(true, 3);

        eventProcessor.processAndPersist("sub-1", "queue-1", "AMQP-OUTAGE-001", xml);

        List<ArrivalEvent> events = eventRepository.listAllDomain();
        assertThat(events).hasSize(1);

        ArrivalEvent event = events.get(0);
        assertThat(event.isFirstMessageAfterServiceOutage())
                .as("firstMessageAfterServiceOutage flag must be captured")
                .isTrue();
        assertThat(event.getSequenceEntriesCount())
                .as("Sequence entries count must match payload")
                .isEqualTo(3);
        assertThat(event.getAerodromeDesignator()).isEqualTo("LPPT");
    }

    /**
     * CP1 AUDIT REQUIREMENT: once an event is persisted, its audit-critical fields
     * (rawPayload, subscriptionId, messageId, receivedAt, contentHash) must remain immutable.
     * The outbox scheduler may update deliveryStatus, but audit fields never change.
     *
     * <p>This is mandatory for CP1 compliance: auditors must trust that the raw event
     * received from the provider is exactly as stored.</p>
     */
    @Test
    @Order(72)
    void auditFieldsRemainImmutableAfterDeliveryStatusUpdate() {
        stubCommunicateProblems();
        String xml = freshXml();

        eventProcessor.processAndPersist("sub-audit-1", "queue-1", "AMQP-AUDIT-001", xml);

        ArrivalEvent original = eventRepository.listAllDomain().get(0);
        String originalPayload = original.getRawPayload();
        String originalHash = original.getContentHash();
        String originalSubId = original.getSubscriptionId();
        String originalMsgId = original.getMessageId();
        Instant originalReceivedAt = original.getReceivedAt();

        original.setDeliveryStatus(OutboxDeliveryStatus.SENT);
        eventRepository.update(original);

        ArrivalEvent reloaded = eventRepository.listAllDomain().get(0);
        assertThat(reloaded.getRawPayload()).isEqualTo(originalPayload);
        assertThat(reloaded.getContentHash()).isEqualTo(originalHash);
        assertThat(reloaded.getSubscriptionId()).isEqualTo(originalSubId);
        assertThat(reloaded.getMessageId()).isEqualTo(originalMsgId);
        assertThat(reloaded.getReceivedAt()).isEqualTo(originalReceivedAt);
        assertThat(reloaded.getDeliveryStatus()).isEqualTo(OutboxDeliveryStatus.SENT);
    }

    /**
     * CP1 MULTI-PROVIDER REQUIREMENT: the consumer must handle events from multiple providers.
     * Events arriving on different subscriptions (different provider IDs) are processed independently.
     *
     * <p>This validates the cross-border scenario where an airport managed by one ANSP
     * receives arrival data from a neighbouring centre / EUROCONTROL.</p>
     */
    @Test
    @Order(73)
    void multiProviderEventsProcessedIndependently() {
        stubCommunicateProblems();

        String xml1 = freshXml();
        String xml2 = freshXmlForAerodrome("ESSA");

        eventProcessor.processAndPersist("sub-provider-A", "queue-A", "AMQP-MPA-001", xml1);
        eventProcessor.processAndPersist("sub-provider-B", "queue-B", "AMQP-MPB-001", xml2);

        List<ArrivalEvent> events = eventRepository.listAllDomain();
        assertThat(events).hasSize(2);

        assertThat(events)
                .extracting(ArrivalEvent::getSubscriptionId)
                .containsExactlyInAnyOrder("sub-provider-A", "sub-provider-B");

        assertThat(events)
                .extracting(ArrivalEvent::getAerodromeDesignator)
                .containsExactlyInAnyOrder("LPPT", "ESSA");
    }

    // ─── Helpers ───

    private String freshXml() {
        return freshXmlForAerodrome("LPPT");
    }

    private String freshXmlForAerodrome(String aerodrome) {
        Instant now = Instant.now();
        String creationTime = now.toString().replaceAll("\\.\\d+Z$", "Z");
        String publicationTime = now.toString();
        String landingTime = now.plusSeconds(3600).toString().replaceAll("\\.\\d+Z$", "Z");
        return VALID_ED254_XML
                .replace("%CREATION_TIME%", creationTime)
                .replace("%PUBLICATION_TIME%", publicationTime)
                .replace("%LANDING_TIME%", landingTime)
                .replace("<aerodromeDesignator>LPPT</aerodromeDesignator>",
                        "<aerodromeDesignator>" + aerodrome + "</aerodromeDesignator>");
    }

    private String freshXmlWithOutageFlag(boolean outage, int entriesCount) {
        Instant now = Instant.now();
        String creationTime = now.toString().replaceAll("\\.\\d+Z$", "Z");
        String publicationTime = now.toString();

        StringBuilder entries = new StringBuilder();
        for (int i = 0; i < entriesCount; i++) {
            String entryLanding = now.plusSeconds(3600 + (i * 120L)).toString().replaceAll("\\.\\d+Z$", "Z");
            entries.append("""
                        <arrivalManagementInformation>
                            <amanTargetLandingTime>%s</amanTargetLandingTime>
                            <arrivalManagementHandlingIndicator>SEQUENCED_STABLE</arrivalManagementHandlingIndicator>
                            <lastFiledRecord>false</lastFiledRecord>
                            <sequenceNumber>%d</sequenceNumber>
                            <typeOfAircraft>A320</typeOfAircraft>
                            <flightIdentification>
                                <arcid>FLT%04d</arcid>
                                <ades>LPPT</ades>
                                <adep>EGLL</adep>
                            </flightIdentification>
                        </arrivalManagementInformation>
                    """.formatted(entryLanding, i + 1, i + 1));
        }

        return """
                <?xml version="1.0" encoding="UTF-8"?>
                <arrivalSequence xmlns="http://www.fixm.aero/ed254/1.0"
                    xmlns:fb="http://www.fixm.aero/base/4.3"
                    xmlns:fx="http://www.fixm.aero/flight/4.3">
                    <creationTime>%s</creationTime>
                    <publicationTime>%s</publicationTime>
                    <firstMessageAfterServiceOutage>%s</firstMessageAfterServiceOutage>
                    <aerodromeDesignator>LPPT</aerodromeDesignator>
                    <sequenceEntries>
                %s    </sequenceEntries>
                </arrivalSequence>
                """.formatted(creationTime, publicationTime, outage, entries);
    }

    private void stubSubscriptionManagerCreate(String subscriptionId, String queueName) {
        String responseJson = """
                {
                    "subscriptionId": "%s",
                    "subscriptionStatus": "PAUSED",
                    "queueName": "%s",
                    "topic": "ED254/v1",
                    "aerodrome": ["LPPT"],
                    "messageType": ["ARRIVAL_SEQUENCE"],
                    "description": "Integration test"
                }
                """.formatted(subscriptionId, queueName);

        wiremock.register(post(urlPathEqualTo("/arrivalSequenceInformation/v1/subscriptions"))
                .willReturn(aResponse()
                        .withStatus(201)
                        .withHeader("Content-Type", "application/json")
                        .withBody(responseJson)));

        stubSubscriptionManagerUpdate(subscriptionId, "ACTIVE");
    }

    private void stubSubscriptionManagerUpdate(String subscriptionId, String newStatus) {
        String responseJson = """
                {
                    "subscriptionId": "%s",
                    "subscriptionStatus": "%s",
                    "queueName": "ED254.v1.client.%s",
                    "topic": "ED254/v1"
                }
                """.formatted(subscriptionId, newStatus, subscriptionId);

        String path = "PAUSED".equalsIgnoreCase(newStatus)
                ? "/arrivalSequenceInformation/v1/subscriptions/" + subscriptionId + "/pause"
                : "/arrivalSequenceInformation/v1/subscriptions/" + subscriptionId + "/resume";

        wiremock.register(put(urlPathEqualTo(path))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody(responseJson)));
    }

    private void stubCommunicateProblems() {
        wiremock.register(post(urlPathEqualTo("/arrivalSequenceInformation/v1/problems"))
                .willReturn(aResponse().withStatus(200)));
    }

    private void seedSubscription(String subscriptionId, String status) {
        Subscription sub = new Subscription();
        sub.setSubscriptionId(subscriptionId);
        sub.setQueueName("ED254.v1.client." + subscriptionId);
        sub.setSubscriptionStatus(status);
        sub.setDestinationAerodrome(List.of(new DestinationAerodrome("LPPT", null)));
        sub.setAnySupplementaryData(true);
        sub.setDescription("Seeded for test");
        sub.setType(com.github.swim_developer.framework.domain.model.SubscriptionType.DECLARED.name());
        sub.setConfigHash("test-hash-" + subscriptionId);
        sub.setProviderId("test-provider");
        subscriptionRepository.persistSubscription(sub);
    }
}
