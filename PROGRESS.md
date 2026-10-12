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
9. Confirm before outward-facing/irreversible actions. No git repo yet — ask before `git init`/commits.

## Environment facts (as of 2026-10-12)
- Project dir: `/Users/nikhil.kadiyan/tsft_projects/personal/java_order`
- Java 17 (OpenJDK 17.0.8), Docker 29.4.0 present. Local `gradle` is 6.8.3 (too old for Spring Boot 3, needs 7.5+/8) and `mvn` is not installed → always use the Gradle wrapper (`./gradlew`), generated via start.spring.io, never the system `gradle`.
- Not a git repository (yet).

## Decisions made
| Decision | Choice | Why |
|---|---|---|
| Saga style | Choreography | Simplest; orchestration is Step 12 stretch |
| Build | Gradle (Kotlin DSL) multi-project + wrapper | User's choice; wrapper avoids old local Gradle |
| Spring Boot | 3.x (Java 17) | Matches installed JDK |
| Kafka | KRaft, no ZooKeeper | Modern, fewer containers |
| DB | One Postgres container, 3 databases | Cheap but logically separated |
| Outbox relay | Polling first, Debezium later | Understand mechanics before CDC |
| Kafka key | orderId | Per-order ordering |
| Tests | Testcontainers | Real infra |

## Step status
| Step | Title | Status |
|---|---|---|
| 0 | Architecture & plan | Plan written; **awaiting user review/OK** |
| 1 | Skeleton, infra, Order REST + JPA | Not started |
| 2 | First event, naive (break it) | Not started |
| 3 | Inventory service | Not started |
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
Step 0 only: `PLAN.md` and this file exist. No code yet. Next action: user reviews architecture, then agent begins Step 1 following the working agreement.

## Per-step notes (append below as steps complete)
<!-- Format: ### Step N — what was built, key files, gotchas hit, questions the user asked + answers worth keeping, open follow-ups -->

## Open questions / follow-ups
- Package base name / groupId (propose `com.example.orders` unless user prefers otherwise)
