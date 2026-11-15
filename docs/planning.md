# Phases

[mission](mission.md) → [requirements](requirements.md) → **phases**

Rules:
1. Build only the current phase.
2. Every PR cites IDs: `Meets: MSG-1`.
3. Phase is done when its exit test passes.
4. Requirement wrong or missing? Open an issue first.

**Current: Phase 1.**

## 0 — Scaffold (shipped)

TCP broker, sessions, ring buffer, CLI. Phase 1 replaces sessions and the ring buffer. Delete, don't adapt.

## 1 — Durable inbox

MSG-1..5, SEC-1, API-1.
**Exit:** message to an offline address survives a broker restart and arrives once.

## 2 — Tasks and runner

TSK-1..4, RUN-1..5, SEC-2, SEC-3.
**Exit:** task sent from host A to `codex@b` while B is down runs when B comes up; result lands on A.

## 3 — MCP and questions

API-2, API-3.
**Exit:** worker calls `ask` mid-task, coordinator wakes and replies, worker continues.

## 4 — Fleet ops

OPS-1, OPS-2, SEC-4.
**Exit:** operator rebuilds any task's history from `log`.
