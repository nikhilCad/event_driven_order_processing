# Step 3 — Inventory service (consumer + producer)

## Goal / Why
First real service-to-service collaboration through events, each service with its own DB. inventory-service consumes `OrderCreated`, reserves stock in `inventory_db`, answers with `StockReserved` or `StockReservationFailed`. Also the first real concurrency risk (overselling the last units).

## Architecture focus
```
order-service ──OrderCreated──► [Kafka orders.events] ──► inventory-service ──► [inventory_db: stock, reservations]
                                                                │
                                [Kafka inventory.events] ◄──────┘  StockReserved | StockReservationFailed
order-service does not consume inventory events yet (Step 4). Publishing is still naive/dual-write (Step 5 fixes it).
```

## What we built
- `common-events`: `StockReservedEvent`, `StockReservationFailedEvent` (with `reason`), `Topics.INVENTORY_EVENTS`.
- New module `inventory-service` (port 8081, package `com.nikhil.orders.inventory`):
  - Flyway `V1__create_stock_and_reservations.sql`: `stock(product_id PK, available CHECK >= 0)`, `reservations(id, order_id, product_id, quantity, status, created_at)` with a **non-unique** index on `order_id` (deliberate: see Break-it #2).
  - `domain/`: `Stock`, `StockRepository.reserve(productId, qty)` = `@Modifying @Query` atomic `UPDATE stock SET available = available - :qty WHERE product_id = :id AND available >= :qty` (returns rows updated), `Reservation`, `ReservationRepository`, `InventoryService.reserve(OrderCreatedEvent)` (one `@Transactional`: update → if 1 row: insert reservation + publish `StockReserved`; else publish `StockReservationFailed` with reason `INSUFFICIENT_STOCK` or `UNKNOWN_PRODUCT`), `setStock`, `availableFor`.
  - `messaging/`: `OrderCreatedListener` (`@KafkaListener` group `inventory-service`, optional `demo.processing-delay-ms` sleep AFTER the transaction), `InventoryEventPublisher` (naive, key = orderId), `KafkaTopicConfig` (`orders.events`, `inventory.events`, 3 partitions).
  - `web/`: `StockController` `PUT/GET /stock/{productId}` to set/read stock for demos.
  - Config: concurrency via `INVENTORY_LISTENER_CONCURRENCY` (default 1).
- order-service: removed the throwaway `OrderCreatedLogger` and the unused consumer config.
- Tests (`InventoryServiceIntegrationTest`, Testcontainers Postgres + Kafka 4.0.0):
  1. 50 concurrent `reserve()` calls on stock 10 → exactly 10 reserved, available 0, 10 reservation rows.
  2. End-to-end through Kafka: reserved / `UNKNOWN_PRODUCT` / `INSUFFICIENT_STOCK` events arrive keyed by orderId and stock decreases.

## How to run and verify
```
docker compose up -d
./gradlew :order-service:bootRun            # :8080
./gradlew :inventory-service:bootRun        # :8081
curl -X PUT localhost:8081/stock/p-1 -H 'Content-Type: application/json' -d '{"available":10}'
curl -X POST localhost:8080/orders -H 'Content-Type: application/json' -d '{"customerId":"c","productId":"p-1","quantity":2,"totalAmount":10}'
curl localhost:8081/stock/p-1
docker exec java_order-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic inventory.events --from-beginning --timeout-ms 4000 --property print.key=true
./gradlew test                              # needs Docker
```
Kill/redelivery demo: start inventory with `DEMO_PROCESSING_DELAY_MS=25000`, POST an order, wait for the "RESERVED" log line, `kill -9` the JVM, restart without the delay.

## Best practices applied
- Business failure (out of stock) is an **event**, not an exception (an exception would retry forever).
- Atomic conditional UPDATE lets the DB arbitrate the race (no read-then-write); `CHECK (available >= 0)` is a second safety net.
- Each service owns its DB; no sharing with order-service.
- Consumer group per service (`inventory-service`).
- Test concurrency at the service level with real Postgres.

## Pitfalls
- **Read-then-write oversell (experiment, not committed):** same 50-thread race with `SELECT available` then `UPDATE ... SET available = ?` reserved **50 units from a stock of 10** and left stock at 5 (lost updates). The atomic UPDATE gives exactly 10.
- With `concurrency=1` the listener is sequential, so the race would not show via Kafka; it appears with `concurrency>1`, multiple instances, or concurrent callers (as in the test).
- A brand-new group with `auto-offset-reset=earliest` **replays the whole topic** (it processed old Step 2 events, all failing with `UNKNOWN_PRODUCT` because no stock existed).
- Both event types go to one topic with no type discriminator yet; Step 4 must add one (consumer needs to know which class to deserialize).
- Publishing is still inside the tx (naive): a consumer can see `StockReserved` before the DB commit. Step 5.

## Break-it results
1. Concurrency: atomic version → exactly 10/50 reserved; naive version → 50/50 (see above).
2. Kill mid-message: consumer slept after the DB commit; `kill -9`. Offsets showed partition 2 **lag 1**. After restart (Kafka waits for the dead member's session timeout, ~45 s) the same `OrderCreated` was **redelivered and processed again**: 2 reservation rows for one order and stock decremented twice (4 → 3 → 2 instead of 3). Confirms at-least-once; motivates **Step 6 idempotent consumers**.

## Q&A (user questions)
(Questions asked during Step 3's open-floor phase are appended here.)

## Known gaps → later steps
- Not idempotent (duplicate delivery double-reserves) → Step 6 (unique `order_id`/processed-events table, dedupe by `eventId`).
- Dual-write publishing → Step 5 (outbox). `eventId` regenerated per publish.
- No release of reservations yet → Step 7 (compensation).
- No `eventType` discriminator; no retries/DLT (Step 8).
