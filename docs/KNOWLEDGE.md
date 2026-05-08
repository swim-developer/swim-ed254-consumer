# swim-ed254-consumer — Knowledge Base


## What This Is

**ANSP role.** Consumes ED-254 (EUROCAE) Arrival Sequence data from Extended AMAN providers (e.g., LFV Sweden / COOPANS) via AMQP 1.0. Data model: FIXM 4.3 (not AIXM). Same framework patterns as DNOTAM consumer, different domain.

~73 classes. 26 unit + 24 integration tests.

## Differences vs DNOTAM Consumer

| Aspect | DNOTAM Consumer | ED-254 Consumer |
|--------|-----------------|-----------------|
| Data model | AIXM 5.1.1 XML | FIXM 4.3 XML |
| Event frequency | Dozens/day | Hundreds/second |
| Latency SLA | < 5s | < 500ms |
| Kafka output | 6 business topics | `ed254-arrival-sequence-topic`, `ed254-provider-exception-topic` |
| CP1 standard | SWIM Registry / SPEC-170 | EUROCAE ED-254 |

## CRITICAL: Who This Connects To

**NEVER connects to `swim-ed254-provider`.** During dev/test → `swim-ed254-consumer-validator`.

| Config field | Points to |
|---|---|
| `amqpBrokerHost` | `ed254-consumer-validator` Artemis |
| `subscriptionManager.url` | `ed254-consumer-validator` SM API |

## Architecture

Same hexagonal structure as DNOTAM consumer. Replace `dnotam` with `ed254` in package names.

```
com.github.swim_developer.ed254.consumer
├── domain/model/        ArrivalSequence, Subscription, FilterDimension
├── application/usecase/ Ed254SubscriptionUseCase, Ed254EventProcessingUseCase
└── infrastructure/      (amqp, rest, scheduling, persistence, kafka, client)
```

## Framework Wiring

| Framework Abstract | This Repo Implementation |
|---|---|
| `AbstractAmqpConsumerManager` | `AmqpConsumerManager` |
| `AbstractEventProcessor` | `EventProcessor` |
| `SwimInboxStore` EP1 | `KafkaInboxStore` (in `swim-inbox-store-kafka`) |
| `SwimInboxReader` EP2 | `Ed254InboxMessageHandler` (extends `AbstractKafkaInboxReader`, `@Incoming ed254-inbox`) |
| `SwimOutboxRouter` EP3 | `Ed254KafkaOutboxRouter` (in `swim-outbox-kafka-ed254`) |
| `SwimEventExtractor` | `Ed254EventExtractorAdapter` |
| `SwimPayloadValidator` | `Ed254EventDataValidator` |

All resilience features (circuit breaker, heartbeat, self-healing, auto-renewal, idempotency) are identical to the DNOTAM consumer — provided by the framework.

## MongoDB

- DB: `swim-ed254`
- Collections: `inbox_messages` (TTL 30d), `arrival_events` (TTL 90d), `subscriptions`, `dead_letter_queue`

## Build & Run

```bash
cd ../swim-developer-framework && mvn clean install -DskipTests
./mvnw clean package -DskipTests
quarkus dev
./mvnw verify -DskipITs=false
```

## ED-254 Domain Context

ED-254 (EUROCAE) is the standard for **Extended AMAN (Arrival Manager)** — arrival sequence data published by upstream ATSU (Air Traffic Service Unit) to downstream ANSPs for runway optimization. This consumer implements the subscriber side per ED-254 service interface bindings.

Not to be confused with DNOTAM: ED-254 is for AMAN only. DNOTAM uses SWIM Registry Service Definition + SPEC-170.
