# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Install sibling dependencies (first time or after pulling upstream changes)
make sync              # pull + clone deps + install to local Maven repo

# Build (skip tests)
./mvnw clean package -DskipTests

# Unit tests only
./mvnw test

# Unit + integration tests (requires Testcontainers / Podman)
./mvnw verify -DskipITs=false

# Run single test class
./mvnw test -Dtest=Ed254ConsumerTest

# Run single test method
./mvnw test -Dtest=Ed254ConsumerTest#testMethodName

# Dev mode (requires local infra — see below)
./mvnw quarkus:dev

# JaCoCo coverage report
./mvnw test jacoco:report   # output: target/site/jacoco/index.html
```

### Local Infrastructure

```bash
# Generate mTLS certs (one-time)
./certs/generate.sh

# Start all services (MongoDB, Kafka, fake SWIM provider, Artemis broker)
podman compose up --build -d

# Subsequent runs (no Artemis changes)
podman compose up -d
```

Dev URLs: REST API `localhost:8080`, Swagger UI `localhost:8080/swagger-ui`, MongoDB UI `localhost:9082`, Kafka UI (AKHQ) `localhost:9083`, Artemis console `localhost:8165`.

### Sibling Dependencies

This project depends on libraries from sibling repos that must be installed to the local Maven repository before building. Run `make deps` to see the list, or `make sync` to automate everything.

Required repos: `swim-developer-root` (install -N), `swim-fixm-model-ed254`, `swim-developer-framework`, `swim-developer-extensions`.

## Architecture

Quarkus 3 / Java 21 service. Hexagonal architecture (ports & adapters). Consumes ED-254 (EUROCAE Extended AMAN) arrival sequence events from a SWIM provider via AMQP 1.0 over mTLS, validates against ED-254 XSD (FIXM 4.3), persists to MongoDB, and distributes to Kafka topics.

### Package Structure

```
com.github.swim_developer.ed254.consumer
├── domain/model/              # Domain entities: ArrivalEvent, Subscription, FlightSelector, etc.
├── domain/port/out/           # Outbound port interfaces (ArrivalEventStore, SubscriptionStore)
├── application/port/in/       # Inbound port interfaces (ManageSubscriptionPort)
├── application/port/out/      # Application-level outbound ports (SequenceGapCachePort)
├── application/usecase/       # Use cases: Ed254SubscriptionUseCase, Ed254EventProcessingUseCase
├── application/service/       # Application services: filter, validate, parse, persist
├── infrastructure/in/amqp/    # AMQP inbound adapter (Ed254InboxMessageHandler)
├── infrastructure/in/rest/    # JAX-RS resources and DTOs
├── infrastructure/out/client/ # REST client adapter to Subscription Manager
├── infrastructure/out/persistence/  # MongoDB stores (Panache)
├── infrastructure/out/messaging/    # Kafka outbox + sequence gap detector
├── infrastructure/out/xml/          # JAXB unmarshalling + event extraction
└── infrastructure/config/           # Quarkus config adapters
```

### Event Processing Pipeline

AMQP message → Kafka inbox staging → `Ed254InboxMessageHandler` → `Ed254EventProcessingUseCase` (parse FIXM XML → XSD validate → sequence gap check → filter by subscription criteria → persist to MongoDB → route to Kafka outbox by event type).

### Framework Extension Points

This service extends `swim-developer-framework` abstractions:

| Framework Abstract | Implementation |
|---|---|
| `SwimInboxStore` | `KafkaInboxStore` (in `swim-inbox-store-kafka`) |
| `AbstractKafkaInboxReader` | `Ed254InboxMessageHandler` |
| `SwimOutboxRouter` | `Ed254KafkaOutboxRouter` (in `swim-outbox-kafka-ed254`) |
| `SwimEventExtractor` | `Ed254EventExtractorAdapter` |
| `SwimPayloadValidator` | `Ed254EventDataValidator` |

### Kafka Topics

| Topic | Content |
|---|---|
| `ed254-inbox-topic` | Raw events (staging) |
| `ed254-arrival-sequence-topic` | Processed arrival sequences |
| `ed254-provider-exception-topic` | Provider exceptions |
| `ed254-dlq-topic` | Dead letter queue |

### Test Organization

- Unit tests: `src/test/.../unit/` — plain JUnit 5 + Mockito
- Integration tests: `src/test/.../integration/` — `*IT.java`, Quarkus `@QuarkusTest` with Testcontainers + WireMock
- Architecture tests: ArchUnit

## Critical Rules

**Consumer never connects to a Provider directly.** During dev/test the consumer connects to `swim-ed254-consumer-validator` (a fake SWIM provider). The `SWIM_PROVIDERS` config must always point to the validator, never to `swim-ed254-provider`.

**No AI authorship in commits.** Never add `Co-Authored-By` or any AI/tool trailer to commit messages. The sole author is the human developer.

**Never change production code to make tests pass.** Investigate the real defect. Ask before touching production code for test purposes.

**No Java Reflection.** No `Field.setAccessible`, no reflective injection — not in production, not in tests.

**Max 400 lines per file** (except Markdown). If exceeded, modularize.

**One action at a time.** Show what will be done, wait for confirmation before executing. Never bundle multiple instructions or questions.

