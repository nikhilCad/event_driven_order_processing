# Conversation checkpoint — end of Step 3 implementation (2026-10-12)

Saved so the next session can pick up. Read `PROGRESS.md` and `docs/README.md` first.

---

**Steps 0–2 are documented and Step 3 is implemented and verified, with the whole project green on `./gradlew test`.**

**Docs and notes**
- **Step docs:** `docs/` has an index plus one file per step (`docs/steps/step-00` to `step-03`). Each covers what we built, how to verify it, pitfalls, break-it results and every question you asked with its answer.
- **Standing rule:** a step doc, with all your Q&A, is now a required part of every step. It's in `PROGRESS.md`, `PLAN.md` and `CLAUDE.md`.
- **Obsidian notes:**
  - **Spring notes:** new items are the Boot 4 differences, the project-local Gradle setup, local environment gotchas, the atomic update and testing concurrency.
  - **Kafka notes:** new items are redelivery, replay and one topic with several event types.

**Step 3: what I built**
- **New service:** `inventory-service` on port 8081.
- **What it does:** it consumes `OrderCreated` and reserves stock in its own DB. It then emits `StockReserved` or `StockReservationFailed`, with a reason: `INSUFFICIENT_STOCK` or `UNKNOWN_PRODUCT`.
- **Reservation logic:** a single atomic `UPDATE stock SET available = available - n WHERE ... AND available >= n`.
- **Demo endpoint:** `PUT/GET /stock/{productId}` for setting and reading stock.
- **Cleanup:** I removed the throwaway logger from order-service.

**Break-it results**
1. **Concurrency race.**
   - 50 threads reserving 1 unit each from a stock of 10: the atomic version reserved exactly 10.
   - The naive read-then-write version (a throwaway experiment, now deleted) reserved **all 50** and left stock at 5, because the threads overwrote each other's updates.
2. **Kill mid-message.**
   - I killed the consumer after the DB commit but before Kafka recorded the message as handled. The group showed lag 1.
   - After restart, the same order was processed **twice**: 2 reservation rows and stock decremented twice.
   - I left this unfixed on purpose, because the idempotent-consumer fix is Step 6.

**Things worth knowing**
- A new consumer group starts at `earliest` and replayed the old Step 2 events.
- Restart redelivery took about 45 seconds. A `kill -9` doesn't leave the group cleanly, so Kafka waits for the dead member's session timeout.
- Both inventory events share one topic with no type marker. Step 4 has to add one so a consumer knows which class to deserialize.

The compose stack is still running. Run `docker compose down` to stop it.

**Your turn.** Things worth asking about:
- Why the atomic `UPDATE` beats read-then-write, and why `CHECK (available >= 0)` is there too.
- What `@Modifying(clearAutomatically = true)` does.
- Why `reservations.order_id` is non-unique.
- Why business failures are events and not exceptions.
- Why `concurrency=1` hides the race.
- Why the consumer slept after the transaction, in the demo.

Say "next" for Step 4: the payment service and closing the happy-path loop.

---

## State at checkpoint
- Step 3 status: **Implemented, awaiting user Q&A** (append each question + answer to `docs/steps/step-03-inventory-service.md`, and keep the Obsidian notes updated).
- Do not start Step 4 until the user says "next".
- Compose stack (Postgres :5433, Kafka :9092) may still be running; both app processes were stopped.
- Run builds with `./gradlew` (optionally `export GRADLE_USER_HOME=$PWD/.gradle-home`); tests need Docker Desktop running.
