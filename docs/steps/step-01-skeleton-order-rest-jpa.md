# Step 1 — Skeleton, infra, Order REST + JPA

## Goal / Why
Make everything except Kafka boring first: a running Postgres, a Spring Boot service, a persisted `Order`. If REST and JPA work, later bugs are messaging bugs.

## Architecture focus
```
Client ──POST/GET──► [order-service] ──► [orders_db]      (built)
Kafka container up but unused; inventory/payment later
```

## What we built
- Gradle multi-project: root `build.gradle.kts` (plugins declared `apply false`, java 17 toolchain, JUnit platform), `settings.gradle.kts`, wrapper.
- `docker-compose.yml`: Postgres 16.4 (host port **5433**, init script creates `orders_db`, `inventory_db`, `payment_db`), Kafka 3.9.0 KRaft with two listeners (INTERNAL `kafka:19092`, EXTERNAL `localhost:9092`); both healthchecked.
- order-service, package `com.nikhil.orders.order`:
  - `domain/`: `Order` (entity; protected ctor + `create()` factory; UUID id; BigDecimal money; enum STRING status), `OrderStatus`, `OrderRepository` (Spring Data), `OrderService` (`@Transactional`), `OrderNotFoundException`
  - `web/`: `OrderController` (`POST /orders` → 201 + `Location`; `GET /orders/{id}`), `CreateOrderRequest`, `OrderResponse` (records), `GlobalExceptionHandler` (404 as `ProblemDetail`)
  - `resources/`: `application.yml` (env-overridable datasource, `ddl-auto=validate`, `open-in-view=false`), `db/migration/V1__create_orders.sql`
- Test: `OrderApiIntegrationTest` (Testcontainers Postgres): create → read; validation 400; unknown id 404.

## How to run and verify
```
docker compose up -d
export GRADLE_USER_HOME=$PWD/.gradle-home        # optional: keep dependency cache local
./gradlew :order-service:bootRun
curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c-1","productId":"p-1","quantity":2,"totalAmount":59.90}'
./gradlew :order-service:test                     # needs Docker running
```

## Best practices applied
Flyway migrations (not `ddl-auto=update`); `ddl-auto=validate`; DTOs not entities; env-var config with defaults; pinned image versions; `open-in-view=false`; constructor injection; explicit enum STRING; BigDecimal for money; UUID assigned in Java.

## Pitfalls
- **Port clash (hit):** a local Postgres on host :5432 silently took the connection ("role app does not exist"). Fixed by mapping the container to 5433.
- Docker Desktop must be running (compose and Testcontainers).
- Wrapper download timed out once (redirect to another host); later worked.

## Break-it results
Stopped Postgres while the app ran: `POST /orders` hung ~30 s (HikariCP `connectionTimeout`) then returned a bare 500; recovered automatically after restart. Hardening candidate: shorter timeout + map to 503.

## Q&A (user questions)
- **How does `SpringApplication.run(OrderServiceApplication.class)` know what to start?** `@SpringBootApplication` = `@SpringBootConfiguration` + `@EnableAutoConfiguration` + `@ComponentScan`. Component scan from the main class's package (and sub-packages) finds controllers/services/repos/entities. Auto-config reads the classpath: webmvc → Tomcat, JPA+driver → DataSource+Hibernate, Flyway → migrations. `--debug` prints the conditions report. Pitfall: classes outside the base package are silently ignored.
- **What does "below it" mean?** Sub-packages of the main class's package.
- **What is Tomcat?** Embedded web server inside the app's JVM; receives HTTP, hands to controller; comes with `starter-webmvc`.
- **Why `apply false` in root build?** Declares the plugin version once without applying it to the root (no code there); subprojects apply it without a version.
- **Should `/orders/` be hardcoded in the controller? Separate file?** No — it duplicated `@RequestMapping("/orders")`. Fixed with `ServletUriComponentsBuilder.fromCurrentRequest().path("/{id}")`. A separate file would be overkill.
- **Is the controller part of a known pattern?** Layered architecture, MVC (the C), REST conventions (201 + Location), plus DTO, Repository, DI. Hexagonal is a heavier alternative (documented in notes).
- **Should OrderResponse be in a separate file?** Java requires one public top-level type per file; nesting records is allowed but separate files are the convention.
- **What is a DTO?** Plain carrier across a boundary; keeps API independent from the table, avoids leaking fields, stops clients setting id/status.
- **What is JPA?** Jakarta Persistence API (spec); Hibernate implements it; Spring Data JPA generates repositories.
- **Do we implement `OrderRepository.save`?** No — Spring Data builds a proxy delegating to `SimpleJpaRepository` at startup. Derived queries from method names or `@Query`.
- **Are these all just SQL?** Yes, via JDBC, but not 1:1: delayed flush, dirty checking at commit, lazy loading (N+1). See SQL with `logging.level.org.hibernate.SQL=DEBUG`.
- **How is the `.sql` file used? What runs it?** Flyway: `FlywayAutoConfiguration` registers a `flywayInitializer` bean that calls `flyway.migrate()`; JPA's `entityManagerFactory` depends on it so migrations finish first. Tracks applied versions + checksums in `flyway_schema_history`. Never edit an applied migration; add `V2`.
- **What is DAO?** Data Access Object; table-centric persistence wrapper. Repository (DDD) is aggregate/domain-centric. Ours is a Repository.
- **What are these layers?** Controller (HTTP) → Service (logic + tx) → Repository (data) → DB; entity/domain alongside. Packages are `web` + `domain` (package by feature).
- **Architectures in more depth + resources** → written into the Obsidian notes (layered, MVC, REST, hexagonal, clean/onion, resource links).
- **12 follow-up topics** (explained): `@Transactional` (proxy, rollback rules, readOnly, self-invocation trap); factory + protected ctor; `open-in-view`; enum STRING; BigDecimal; UUID in Java; bean validation; `@RestControllerAdvice`/ProblemDetail; constructor injection; Testcontainers `@ServiceConnection`; Kafka dual listeners; Hikari 30 s hang.

## Follow-ups
- Fail-fast DB timeout + 503 mapping (hardening step).
- Compose Kafka is 3.9.0; Testcontainers needs 4.0.0 (see step 2). Align later if desired.
