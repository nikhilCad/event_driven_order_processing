# Step documentation

One file per step in `steps/`. Each documents what was built, why, how to run/verify it, pitfalls hit, break-it results, and **every question the user asked with the answer**. A new agent should read these (after `PROGRESS.md` and `PLAN.md`) to understand how and why the code looks the way it does.

| Step | File | Status |
|---|---|---|
| 0 | [step-00-architecture-and-setup.md](steps/step-00-architecture-and-setup.md) | Done |
| 1 | [step-01-skeleton-order-rest-jpa.md](steps/step-01-skeleton-order-rest-jpa.md) | Done |
| 2 | [step-02-first-event-naive-dual-write.md](steps/step-02-first-event-naive-dual-write.md) | Done |
| 3 | [step-03-inventory-service.md](steps/step-03-inventory-service.md) | Implemented (awaiting Q&A) |

## Per-step doc structure (keep consistent)
1. Goal / Why
2. Architecture focus (diagram)
3. What we built (files, key classes, config)
4. How to run and verify (exact commands)
5. Best practices applied
6. Pitfalls (hit and avoided)
7. Break-it results
8. Q&A — questions the user asked, with answers
9. Decisions and follow-ups for later steps

## Conversation checkpoints
`convo/` holds saved assistant messages for resuming later (e.g. [2026-10-12-step-3-handoff.md](convo/2026-10-12-step-3-handoff.md)).
