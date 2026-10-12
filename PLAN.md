# Event-Driven Order Processing Platform — Learning Plan

Goal: build a distributed order-processing system (Java, Spring Boot, Kafka, PostgreSQL) **to learn**, one dev-cycle step at a time.
Read `PROGRESS.md` first for working agreement + current state. This file is the stable roadmap.

Central question of the whole project:
> Services each own their DB, so there is no global transaction. How do we stay consistent anyway?

Every step below answers one slice of that question.

---

## Step 0 — Architecture (design only, no feature code)

### Services (each owns its own database — "database per service")

| Service | Owns | Consumes | Produces |
|---|---|---|---|
| order-service (REST entry) | `orders`, `outbox` | `StockReserved`, `StockReservationFailed`, `PaymentCompleted`, `PaymentFailed` | `OrderCreated`, `OrderCancelled`, `OrderConfirmed` |
| inventory-service | `stock`, `reservations`, `outbox`, `processed_messages` | `OrderCreated`, `PaymentFailed`/`OrderCancelled` | `StockReserved`, `StockReservationFailed`, `StockReleased` |
| payment-service | `payments`, `outbox`, `processed_messages` | `StockReserved` | `PaymentCompleted`, `PaymentFailed` |

### Final architecture (target at end of Step 11)

```
                    POST /orders
   Client ─────────────────────────────┐
                                       ▼
                             ┌──────────────────┐
                             │  order-service   │──► orders_db (orders + outbox)
                             └──────────────────┘
                                  │ outbox relay
                                  ▼
 ┌──────────────────────────── KAFKA (KRaft) ───────────────────────────────┐
 │  orders.events   inventory.events   payment.events   *.retry   *.DLT     │
 │  key = orderId  → same order always same partition → per-order ordering  │
 └──────────────────────────────────────────────────────────────────────────┘
      ▲   │                    ▲   │                    ▲   │
      │   ▼                    │   ▼                    │   ▼
 ┌──────────────────┐   ┌──────────────────┐   (all consume/produce via
 │ inventory-service│   │  payment-service │    outbox + idempotent consumer)
 └──────────────────┘   └──────────────────┘
   inventory_db            payment_db

 Observability: every service ─► /actuator/prometheus ─► Prometheus ─► Grafana
 Infra: docker compose = Kafka(KRaft) + Postgres + Prometheus + Grafana (+ services)
```

### Saga style chosen: **choreography** (no central coordinator)
Services react to each other's events. Simpler, matches the event list, best for learning the core patterns.
Trade-off: flow is implicit (spread across services) → harder to see/debug. Orchestration (a coordinator service) is a Step 12 stretch.

### Happy path
```
order-service      inventory-service        payment-service         order-service
 OrderCreated ───► reserve stock ──────────► charge ───────────────► mark CONFIRMED
 (PENDING)         StockReserved            PaymentCompleted
```

### Failure path (compensation)
```
 OrderCreated ─► StockReserved ─► PaymentFailed ─► inventory: release stock → StockReleased
                                                └► order: mark CANCELLED
 OrderCreated ─► StockReservationFailed ─► order: mark CANCELLED   (nothing to compensate)
```

### Order state machine
`PENDING → STOCK_RESERVED → CONFIRMED` | `PENDING|STOCK_RESERVED → CANCELLED`

### Event envelope (every event carries)
`eventId` (UUID, used for idempotency), `eventType`, `aggregateId` (= orderId, used as Kafka key), `occurredAt`, `payload`.

### Key design decisions (and why)
- **Kafka key = orderId** → all events of one order land in one partition → ordered per order. Ordering across orders is NOT guaranteed and not needed.
- **At-least-once delivery** is the realistic guarantee → duplicates WILL happen → consumers must be idempotent.
- **Outbox over "write DB then publish"** → the dual-write problem (see Step 2/5).
- **One Postgres container, three databases** → cheap locally, still logically separate (no cross-DB joins/FKs, ever).
- **Java 17 + Spring Boot 4.x, Gradle (Kotlin DSL) multi-project build** (Gradle wrapper; local `gradle` 6.8.3 is too old). Docker is available.
- **Testcontainers** for integration tests (real Kafka/Postgres, no mocks of infrastructure).
- **Kafka mode: KRaft** (no ZooKeeper).

### Step 0 reading (do before Step 1)
- microservices.io: Database-per-Service, Saga, Transactional Outbox, Idempotent Consumer
- Confluent Developer: Kafka 101 (topics, partitions, offsets, consumer groups modules)
- DDIA ch. 7 (transactions) and ch. 8 (distributed failures) — skim

### Step 0 Q&A checkpoint
Review the diagrams with the user. Questions to provoke thought: why no shared DB? why key by orderId? what if payment-service is down for an hour? Do not move on until the user says go.

---

## Per-step template (every step follows this)

1. **Why** — short write-up of the problem this step solves (shown to user BEFORE coding)
2. **Architecture focus** — diagram highlighting which part of the system we're building
3. **Concepts** — what to understand first
4. **Implement** — agent writes code, explaining as it goes; small commits-worth of changes
5. **Best practices / Pitfalls**
6. **Break it** — deliberately cause the failure this step prevents; observe
7. **Q&A floor** — agent STOPS, invites questions about the code; answers until user says "next"
8. **Done criteria + update PROGRESS.md**

---

## Step 1 — Skeleton, infra, Order REST + JPA

**Why:** Foundation. Get a running Postgres + Kafka and one service with a persistent aggregate before adding messaging. Learn Spring Boot + JPA first so Kafka is the only new thing later.
**Architecture focus:** `Client → order-service → orders_db`. Kafka container up but unused.
**Concepts:** Spring Boot auto-config, layered structure (controller/service/repository), JPA entity lifecycle, Flyway migrations, DTO vs entity, `@Transactional`.
**Implement:**
- Gradle multi-project build (`settings.gradle.kts` includes `order-service`, `inventory-service`, `payment-service`, `common-events`) + Gradle wrapper
- `docker-compose.yml`: Kafka (KRaft) + Postgres (init script creates 3 DBs)
- order-service: `POST /orders`, `GET /orders/{id}`, status enum, Flyway `V1__orders.sql`
**Best practices:** Flyway over `ddl-auto`; validate input; return DTOs not entities; `ddl-auto=validate`; externalize config via env vars; pin image versions.
**Pitfalls:** `ddl-auto=update` in anything real; exposing entities; lazy-loading outside a transaction; hardcoded localhost URLs breaking in Docker later.
**Break it:** Stop Postgres mid-request; see the error path and what the client gets.
**Done:** `curl POST /orders` persists a PENDING order; compose up/down works.

## Step 2 — First event, the naive way (and see it break)

**Why:** Feel the dual-write problem before learning its cure. Save to DB then `kafkaTemplate.send()` — two systems, no shared transaction.
**Architecture focus:** `order-service → Kafka → one throwaway consumer`.
**Concepts:** topics, partitions, producer acks, serialization (JSON), `KafkaTemplate`, `@KafkaListener`, consumer group, offset commit.
**Implement:** publish `OrderCreated` after save; a simple logging consumer; shared event classes in `common-events`.
**Best practices:** `acks=all`, `enable.idempotence=true`; explicit topic creation (`NewTopic`) with chosen partitions; key = orderId; version your events.
**Pitfalls:** publishing inside the DB transaction (event sent, then tx rolls back → ghost event); publishing after commit and crashing (order saved, event lost); auto-create topics; Java-serialized payloads; leaking entities into events.
**Break it:** (a) throw after `send()` but before commit; (b) kill the app between commit and send. Observe inconsistency. **This pain motivates Step 5.**
**Done:** event visible in topic (kafka-console-consumer); inconsistency reproduced and written down.

## Step 3 — Inventory service (consumer + producer)

**Why:** Introduce a second service that reacts to events and has its own DB. First real use of consumer groups.
**Architecture focus:** `OrderCreated → inventory-service → inventory_db → StockReserved | StockReservationFailed`.
**Concepts:** consumer group per service (`group.id`), offset commit modes (Spring `AckMode`), local transaction per message, domain failure vs technical failure.
**Implement:** `stock` + `reservations` tables, reserve logic with row-level safety (optimistic version or `SELECT … FOR UPDATE`/atomic `UPDATE … WHERE available >= n`), emit result events (still naive publish).
**Best practices:** out-of-stock is a business outcome → emit a failure event, don't throw; atomic conditional update to avoid oversell; never share DB with order-service.
**Pitfalls:** read-then-write race → overselling; treating business failure as exception → infinite retry; committing offset before processing; sharing tables across services.
**Break it:** fire 50 concurrent orders for stock of 10; verify no oversell. Kill consumer mid-message; see redelivery.
**Done:** order for in-stock item yields `StockReserved`; out-of-stock yields `StockReservationFailed`.

## Step 4 — Payment service + close the loop (happy path end-to-end)

**Why:** Complete the happy path so there's a whole flow to protect with the resilience patterns.
**Architecture focus:** `StockReserved → payment-service → PaymentCompleted|PaymentFailed → order-service updates status`.
**Concepts:** choreography flow, order state machine, simulated external dependency (fake payment gateway with configurable failure rate).
**Implement:** payment-service; order-service consumes inventory/payment events and transitions status; guard illegal transitions.
**Best practices:** state machine with explicit allowed transitions; fake gateway behind an interface; events immutable and past-tense named.
**Pitfalls:** accepting events that arrive out of order and corrupting state; commands disguised as events; tight coupling via shared DTO internals.
**Break it:** replay an old event; observe state corruption (motivates guards + Step 6).
**Done:** `POST /orders` → eventually CONFIRMED; failing payment → eventually still inconsistent (stock stuck reserved) — **that gap motivates Step 7**.

## Step 5 — Transactional Outbox

**Why:** Fix the dual-write bug from Step 2. Write business row + event row in ONE local DB transaction; a separate relay publishes from the outbox table to Kafka.
**Architecture focus:** inside each service: `business tx → (orders + outbox) → relay → Kafka`.
**Concepts:** atomic local transaction, polling publisher, at-least-once publish, outbox table design (id, aggregate_id, type, payload, created_at, published_at), `FOR UPDATE SKIP LOCKED` for multiple relay instances, CDC (Debezium) as the alternative.
**Implement:** outbox table + repository in each service; replace direct `send()`; scheduled relay; cleanup of published rows.
**Best practices:** publish ordered per aggregate; mark published only after broker ack; batch; index on unpublished rows; reusable outbox module/lib.
**Pitfalls:** relay publishes but crashes before marking → duplicate (OK, that's why Step 6); multiple relays double-publishing without SKIP LOCKED; unbounded outbox growth; losing per-aggregate ordering with parallel relays; polling interval vs latency.
**Break it:** crash between commit and relay — event still goes out after restart. Kill relay mid-batch — duplicates appear.
**Done:** Step 2 failure scenarios no longer cause inconsistency.

## Step 6 — Idempotent consumers

**Why:** Outbox + Kafka = at-least-once. Duplicates are guaranteed eventually. Processing twice must equal processing once.
**Architecture focus:** consumer side: `message → check processed_messages → business effect + record eventId in SAME tx`.
**Concepts:** idempotency key (eventId), unique constraint as the guard, natural idempotency vs dedupe table, exactly-once *effect* vs exactly-once delivery.
**Implement:** `processed_messages(event_id PK, consumer, processed_at)` in each consumer service; wrap handler in dedupe tx.
**Best practices:** dedupe record and business write in the SAME transaction; rely on unique-constraint violation not check-then-insert; TTL/cleanup of old ids.
**Pitfalls:** check-then-insert race; recording id outside the business tx (lost work or double work); using offset as id; non-idempotent side effects to external systems (use idempotency keys with the gateway).
**Break it:** publish the same event 5× — one effect. Run two consumers racing on the same event.
**Done:** duplicate injection test passes.

## Step 7 — Saga compensation

**Why:** No global rollback → undo with explicit **compensating transactions**.
**Architecture focus:** failure path in the diagram above.
**Concepts:** forward steps vs compensations, semantic rollback, compensations must be idempotent and retryable, what is/isn't compensable (ordering pivot step), saga state visibility.
**Implement:** `PaymentFailed → inventory releases stock → StockReleased`; order → CANCELLED; `StockReservationFailed → CANCELLED`; saga timeout for stuck orders (scheduled sweep).
**Best practices:** compensation is a new forward event, not a DB rollback; make compensation idempotent; log saga state; handle "compensation arrives before the action" (out-of-order).
**Pitfalls:** compensation that can fail with no retry; releasing stock never reserved; no timeout → orders stuck forever; assuming compensation is instant.
**Break it:** force payment to fail; verify stock restored exactly once even if `PaymentFailed` is delivered twice.
**Done:** failed payment leaves system consistent: order CANCELLED, stock released.

## Step 8 — Retries and Dead-Letter Topics

**Why:** Not all failures are equal. Transient (DB blip) → retry. Poison (malformed message) → park it, don't block the partition.
**Architecture focus:** `topic → consumer → retry topics (backoff) → DLT`.
**Concepts:** `DefaultErrorHandler` + `BackOff`, non-blocking retry (`@RetryableTopic`), `DeadLetterPublishingRecoverer`, retryable vs non-retryable exceptions, head-of-line blocking, DLT headers (original topic, exception).
**Implement:** configure retry/backoff, non-retryable exception list (deserialization/validation), DLT consumer that logs/alerts, replay tool/procedure for DLT.
**Best practices:** exponential backoff with jitter; cap attempts; DLT retention longer; monitor DLT depth; keep DLT messages replayable.
**Pitfalls:** infinite retry on poison message blocking partition; retrying business failures; DLT with nobody watching it; retry storms; breaking per-key ordering with retry topics (know the trade-off).
**Break it:** send garbage bytes; throw transient errors N times then succeed; observe retry timings and DLT landing.
**Done:** poison message lands in DLT, healthy traffic unaffected.

## Step 9 — Partitioning, consumer groups, offsets

**Why:** Scale and understand what Kafka guarantees.
**Architecture focus:** Kafka layer internals; N consumer instances.
**Concepts:** partition count as max parallelism, key-based routing, rebalancing, `concurrency`, commit strategies, `auto.offset.reset`, lag, ordering guarantees, hot partitions.
**Implement:** raise partitions, run 2–3 instances of inventory-service, observe assignment; manual ack where it matters; experiment with static membership / cooperative rebalancing.
**Best practices:** choose partition count up front (can't safely reduce; increasing remaps keys); monitor consumer lag; commit after processing.
**Pitfalls:** more consumers than partitions (idle); changing partitions breaks key→partition mapping; long processing exceeding `max.poll.interval.ms` → rebalance loops; null keys.
**Break it:** kill a consumer, watch rebalance + redelivery; reset offsets with `kafka-consumer-groups`.
**Done:** can explain and demonstrate parallelism, rebalance, and per-order ordering.

## Step 10 — Observability (Prometheus + Grafana)

**Why:** Distributed systems fail silently. Metrics make the invisible visible.
**Architecture focus:** `services → /actuator/prometheus → Prometheus → Grafana`.
**Concepts:** Micrometer, RED metrics, custom business metrics, consumer lag, outbox backlog, DLT depth, correlation/trace IDs in logs.
**Implement:** Actuator + Micrometer registry; custom counters (orders by status, saga compensations, dedupe hits); Prometheus scrape config; Grafana provisioned dashboard; add orderId/eventId to MDC logging.
**Best practices:** low-cardinality labels; alert on outbox age & DLT depth & lag; provision dashboards as code.
**Pitfalls:** high-cardinality labels (orderId as a label!); metrics that don't map to a question; dashboards no one can interpret.
**Done:** dashboard shows throughput, outbox backlog, lag, DLT count, compensations.

## Step 11 — Full Docker Compose + chaos/hardening

**Why:** Reproducible deployment; prove resilience claims.
**Implement:** Dockerfiles (multi-stage), compose profile for full stack, healthchecks, `depends_on` with conditions, README quickstart, scripted scenarios (happy, out-of-stock, payment fail, duplicate, kill-service).
**Best practices:** healthchecks not sleeps; non-root image user; config by env; one-command up.
**Pitfalls:** services starting before Kafka/DB ready; `localhost` vs service names / Kafka advertised listeners (classic!); volumes hiding stale state.
**Done:** `docker compose up` brings everything; chaos scenarios documented with expected results.

## Step 12 — Stretch

- Debezium outbox event router (CDC instead of polling)
- Orchestrated saga (coordinator) and compare to choreography
- Schema Registry + Avro/Protobuf, schema evolution
- Kafka transactions / exactly-once semantics (and why outbox still matters)
- OpenTelemetry distributed tracing

---

## Resource index
- Kafka: developer.confluent.io (Kafka 101; Spring + Kafka course); *Kafka: The Definitive Guide*
- Spring: spring.io/guides; docs.spring.io/spring-kafka (KafkaTemplate, @KafkaListener, error handlers, DeadLetterPublishingRecoverer, @RetryableTopic)
- Patterns: microservices.io; *Microservices Patterns* (Richardson); *DDIA* (Kleppmann) ch. 5–9, 11
- Debezium Outbox Event Router docs
- Observability: Micrometer, Prometheus, Grafana getting-started docs
- Testing: Testcontainers (Kafka, PostgreSQL modules)
