# Step 2 — First event, the naive way (and watch it break)

## Goal / Why
Feel the dual-write problem before learning its cure: save to Postgres, then publish to Kafka — two systems, no shared transaction.

## Architecture focus
```
Client ─POST─► [order-service] ──① INSERT──► [orders_db]
                     └─────────② send ─────► [Kafka orders.events] ──► throwaway logging consumer
                                              key = orderId, 3 partitions
① and ② are not atomic.
```

## What we built
- `common-events` module (plain Java, no Spring): `OrderCreatedEvent` record (eventId, orderId, customerId, productId, quantity, totalAmount, occurredAt), `Topics.ORDER_EVENTS = "orders.events"`.
- order-service:
  - dep `spring-boot-starter-kafka`, `project(":common-events")`
  - `messaging/KafkaTopicConfig`: `NewTopic` `orders.events`, 3 partitions, RF 1
  - `messaging/OrderEventPublisher`: `KafkaTemplate<String, OrderCreatedEvent>.send(topic, orderId, event)`, `whenComplete` logs result
  - `messaging/OrderCreatedLogger`: `@KafkaListener` group `order-debug-logger` (throwaway — replaced in Step 3)
  - `OrderService.createOrder`: save, then publish inside the transaction; two TEMPORARY demo flags (`demo.fail-after-publish`, `demo.publish-after-commit`, env `DEMO_FAIL_AFTER_PUBLISH` / `DEMO_PUBLISH_AFTER_COMMIT`)
  - `application.yml`: producer `acks=all`, `enable.idempotence=true`, `max.block.ms=5000`, `JacksonJsonSerializer`, no type headers; consumer `earliest`, `JacksonJsonDeserializer` with `spring.json.value.default.type` + trusted package
- Test: `OrderApiIntegrationTest` now also starts Kafka (Testcontainers `apache/kafka:4.0.0`) and asserts an event keyed by orderId arrives.

## How to run and verify
```
docker compose up -d
./gradlew :order-service:bootRun
# POST an order, then:
docker exec java_order-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.events --from-beginning --timeout-ms 5000
docker exec java_order-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group order-debug-logger
```

## Best practices applied
Explicit topic creation; key = orderId; `acks=all` + idempotence; no Java class names in headers; events in a separate module from entities; JSON not Java serialization.

## Pitfalls
- Boot 4 / Spring Kafka 4.1.1 use **Jackson 3** → `JacksonJsonSerializer/Deserializer` (old `JsonSerializer` is Jackson 2, many tutorials show it).
- Testcontainers 2.x `KafkaContainer` with `apache/kafka:3.9.0` fails ("advertised.listeners cannot use 0.0.0.0"); use `4.0.0`.
- `eventId` is generated per publish call (flaw): a re-publish gets a new id → consumers can't dedupe. Fix in Step 5 (store id in outbox row).
- No `ErrorHandlingDeserializer`: a poison message would loop the consumer (Step 8).

## Break-it results
1. `DEMO_FAIL_AFTER_PUBLISH=true`: HTTP 500, **0 orders in DB, 1 event on topic** → ghost event.
2. `DEMO_PUBLISH_AFTER_COMMIT=true` + Kafka stopped: HTTP 500, **order in DB (PENDING), 0 events** → lost event; client thinks it failed and retries → duplicate order.
3. Surprise (my prediction was wrong): publishing inside the tx with Kafka down, cold or warm producer → `send()` throws `TimeoutException` after `max.block.ms` → tx rolls back → consistent but 5 s + 500. The client refreshes metadata on disconnect, so no silent buffering.
Conclusion: no ordering of save and send is safe → Transactional Outbox (Step 5). The demo flags are removed then.

## Q&A (user questions)
- **What is `publishAfterCommit`?** Temporary demo switch: false = publish immediately inside the tx (before commit); true = register an `afterCommit` callback via `TransactionSynchronizationManager`. Exists only to reproduce both failures.
- **Why "it disappears in Step 5"?** With the outbox the service never publishes; it writes order + outbox row in one tx, a relay publishes. No "when to publish" choice, no ghost (rolled back together), no loss (event persisted).
- **How is a partition chosen for an orderId?** `toPositive(murmur2(keyBytes)) % numPartitions` — same key, same partition → per-order ordering. Changing partition count remaps keys; null key = no per-key order.
- **Where is that logic written?** Not in our code: `org.apache.kafka.clients.producer.internals.BuiltInPartitioner.partitionForKey` in `kafka-clients` (verified with `javap`). Hashes the serialized bytes.
- **Is consumers = partitions here?** No: 3 partitions, 1 consumer instance reads all 3. Rule: within a group each partition goes to at most one consumer; max useful consumers = partitions; extras idle; different groups each get every message.
- **If one consumer reads 3 partitions, what is Kafka's use?** Partitions = capacity to scale later without producer/code change. Kafka also gives decoupling, durable buffer, replay, fan-out, per-key ordering.
- **How does order-service handle data from 3 partitions concurrently?** It doesn't, on the consuming side: `concurrency=1` → one listener thread polls all partitions and processes records sequentially (log shows one thread `ntainer#0-0-C-1`). The producing side is concurrent (many Tomcat threads share one thread-safe producer). `concurrency=N` → N threads, one partition each; handlers must be thread-safe.
- **What is the `${demo.fail-after-publish:false}` string in `@Value`?** Property placeholder: key and default after the colon; sources: CLI args > env vars > yml; `DEMO_FAIL_AFTER_PUBLISH` maps automatically. `@ConfigurationProperties` for groups.
- **Main things missed (answered):** `acks=all` vs idempotence (what each covers and does not — our own double-send is not covered → idempotent consumers); offsets (per-partition sequence numbers, group bookmark in `__consumer_offsets`, lag, commit-after-batch = at-least-once; verified with `kafka-consumer-groups.sh`); `eventId` vs `orderId` (one order → many events; dedupe per event; create once and store).

## Follow-ups
- Step 3: replace `OrderCreatedLogger` with a real consumer in inventory-service.
- Step 5: remove demo flags; make `eventId` stable (outbox).
- Step 4/5: add an `eventType` discriminator when `orders.events` carries several event types.
