# PROGRESS & Agent Handoff

**New agent: read this file first, then `PLAN.md`. Continue from "Current state".**

## Purpose
Learning project. The user is experienced in Java but NEW to system design, Kafka, Spring Boot. Teaching quality matters more than speed.

## Working agreement (must follow every step)
1. **Before coding a step:** show a short "Why we're doing this" write-up + an architecture diagram highlighting the part being built (from `PLAN.md`). Keep it brief.
2. **Implement the step's code** yourself, in small pieces, explaining non-obvious choices as you go.
3. **Cover best practices and common pitfalls** for the step (listed in `PLAN.md`; add new ones discovered).
4. **Do the "Break it" exercise** where the plan lists one, so the user sees the failure the pattern prevents.
5. **Open the floor:** after implementing, STOP. Invite questions about the code. Answer until the user says to move on. Do not start the next step unprompted.
6. **Update this file** at the end of each step (status table, notes, decisions, open questions).
7. Code comments: minimal; only explain WHY when non-obvious (user's global rule). Teaching goes in chat and docs, not in code comments.
8. Tests: test logic and behavior, not framework plumbing. Prefer Testcontainers for infra; with Jest-style mocks use requireActual where applicable (n/a for Java).
9. **Notes:** after each step and each Q&A answer, update VERY short, precise notes (no verbosity) in `~/tsft_projects/personal/obsidian-notes/System Design/03 Java Spring 6 with Spring Boot 3 - Telusko/02 Springboot/` — `02 Spring Boot Notes (java_order).md`, `03 Kafka Notes (java_order).md` (Java notes there too if relevant). **Never blind-append:** read the file, put the note under the right numbered section/heading, merge with existing topics instead of duplicating, add a new section only when none fits, keep the Contents line current.
10. **Step docs (prerequisite for every step):** each step gets `docs/steps/step-NN-<slug>.md` (structure in `docs/README.md`: goal, architecture, what we built, how to verify, best practices, pitfalls, break-it results, **Q&A with every user question + answer**, follow-ups). Create it when the step starts, fill it as you build, append each Q&A during the floor-open phase, and keep the `docs/README.md` index current. A new agent must be able to continue from the docs alone.
11. Confirm before outward-facing/irreversible actions. Ask before commits.

## Environment facts (as of 2026-10-12)
- Project dir: `/Users/nikhil.kadiyan/tsft_projects/personal/java_order`
- Java 17 (OpenJDK 17.0.8), Docker 29.4.0 present. Local `gradle` is 6.8.3 (too old for Spring Boot 3, needs 7.5+/8) and `mvn` is not installed → always use the Gradle wrapper (`./gradlew`), generated via start.spring.io, never the system `gradle`.
- Git repo now exists; ask before committing.
- Local Postgres already listens on host :5432 → our compose Postgres is mapped to **host port 5433**.
- Gradle isolation (user request: nothing global): `gradle-wrapper.properties` uses `distributionBase=PROJECT`/`zipStoreBase=PROJECT` so the Gradle 9.8.1 binary lives in `.gradle/wrapper/dists/` (gitignored). Never touch `~/.gradle/wrapper/dists` (user's other projects). Dependency cache: optionally `export GRADLE_USER_HOME=$PWD/.gradle-home` (gitignored) to keep that local too. If the wrapper download times out, curl the zip from services.gradle.org, verify its sha256, and seed it at `.gradle/wrapper/dists/gradle-9.8.1-bin/<md5-base36 of URL>/`.
- Docker Desktop must be running (Testcontainers + compose).

## Decisions made
| Decision | Choice | Why |
|---|---|---|
| Saga style | Choreography | Simplest; orchestration is Step 12 stretch |
| Build | Gradle (Kotlin DSL) multi-project + wrapper | User's choice; wrapper avoids old local Gradle |
| Spring Boot | 4.1.1 (Java 17) | start.spring.io offers only 4.x now; 4.x modular starters (`starter-webmvc`, `starter-flyway`), Testcontainers 2.x, Jackson 3 |
| Kafka | KRaft, no ZooKeeper | Modern, fewer containers |
| DB | One Postgres container, 3 databases | Cheap but logically separated |
| Outbox relay | Polling first, Debezium later | Understand mechanics before CDC |
| Kafka key | orderId | Per-order ordering |
| Tests | Testcontainers | Real infra |
| Base package / groupId | `com.nikhil.orders` | User's choice |

## Step status
| Step | Title | Status |
|---|---|---|
| 0 | Architecture & plan | Plan written; **awaiting user review/OK** |
| 1 | Skeleton, infra, Order REST + JPA | Done |
| 2 | First event, naive (break it) | Done |
| 3 | Inventory service | In progress |
| 4 | Payment + happy path E2E | Not started |
| 5 | Transactional Outbox | Not started |
| 6 | Idempotent consumers | Not started |
| 7 | Saga compensation | Not started |
| 8 | Retries + DLT | Not started |
| 9 | Partitioning / groups / offsets | Not started |
| 10 | Observability | Not started |
| 11 | Full compose + chaos | Not started |
| 12 | Stretch | Not started |

Statuses: Not started → In progress → Implemented (awaiting Q&A) → Done (user said next).

## Current state
Steps 0–2 done and documented in `docs/steps/`. Step 3 (inventory service) in progress. Compose stack may be running (`docker compose down` to stop).

## Per-step notes (append below as steps complete)
<!-- Format: ### Step N — what was built, key files, gotchas hit, questions the user asked + answers worth keeping, open follow-ups -->

### Step 1 — Skeleton, infra, Order REST + JPA (done; full detail + Q&A in `docs/steps/step-01-*.md`)
- Built: Gradle multi-project (only `order-service` included for now; other modules added when first used), `docker-compose.yml` (Postgres 16.4 on 5433 with init.sql creating orders_db/inventory_db/payment_db, Kafka 3.9.0 KRaft with INTERNAL `kafka:19092` / EXTERNAL `localhost:9092` listeners, both healthchecked), order-service (`POST /orders`, `GET /orders/{id}`, Flyway `V1__create_orders.sql`, `ddl-auto=validate`, `open-in-view=false`, ProblemDetail 404), Testcontainers integration test `OrderApiIntegrationTest` (passes).
- Key files: `order-service/src/main/java/com/nikhil/orders/order/{domain,web}`, `order-service/src/main/resources/{application.yml,db/migration}`.
- Verified manually: 201 + PENDING row in orders_db; 400 on invalid input.
- Break-it result: Postgres stopped → POST returns 500 after ~30s (Hikari `connectionTimeout` default), recovers automatically after restart. Lesson: fail fast (shorter timeout, map to 503) — not done yet, candidate for hardening.
- Gotcha hit: host Postgres on 5432 silently captured the connection ("role app does not exist") — the classic port-clash pitfall.
- Test run (`./gradlew :order-service:test`) needs Docker running.

### Step 2 — First event, naive dual-write (done; full detail + Q&A in `docs/steps/step-02-*.md`)
- Built: `common-events` module (`OrderCreatedEvent`, `Topics.ORDER_EVENTS`); order-service: `spring-boot-starter-kafka`, `KafkaTopicConfig` (`orders.events`, 3 partitions, RF 1), `OrderEventPublisher` (key = orderId, async send + `whenComplete` log), throwaway `OrderCreatedLogger` consumer (group `order-debug-logger`, **delete/replace in Step 3**), `OrderService` publishes inside the tx. Producer: acks=all, idempotence on, `max.block.ms=5000`, no type headers (consumer uses `spring.json.value.default.type`).
- Boot 4 / Spring Kafka 4.1.1 uses Jackson 3 → `JacksonJsonSerializer/Deserializer` (old `JsonSerializer` = Jackson 2, many tutorials show it).
- Demo switches (TEMPORARY, remove in Step 5): `demo.fail-after-publish` (env `DEMO_FAIL_AFTER_PUBLISH=true`), `demo.publish-after-commit` (env `DEMO_PUBLISH_AFTER_COMMIT=true`).
- Break-it results:
  1. fail-after-publish: HTTP 500, **0 orders in DB, 1 event on topic** (ghost event).
  2. publish-after-commit + Kafka down: HTTP 500 (client thinks it failed), **order in DB as PENDING, 0 events** (lost event + client likely retries → duplicate order).
  3. Surprise: publish inside the tx with Kafka down (cold OR warm producer) → `send()` throws `TimeoutException` after `max.block.ms` → tx rolls back → consistent but 5s + 500. Kafka client refreshes metadata on disconnect, so no silent buffering.
- Test: `OrderApiIntegrationTest` now also starts Kafka (Testcontainers) and asserts the event arrives keyed by orderId. **Use `apache/kafka:4.0.0` in tests**: `apache/kafka:3.9.0` fails with Testcontainers 2.x ("advertised.listeners cannot use 0.0.0.0"). Compose still uses 3.9.0 (works; align later if desired).
- Event envelope: only one event type so far, so no `eventType` discriminator yet — add when `orders.events` carries several types (Step 4/5).
- Pitfalls to remember: no `ErrorHandlingDeserializer` yet → a poison message would loop the consumer (Step 8); topic auto-create avoided via `NewTopic`.

## Open questions / follow-ups
