---
type: patch
---

Production telemetry now uses one stable eleven-name contract across native tracing and JavaScript callbacks.

SQL is hard-cut to `lix.sql.query`, `lix.sql.batch`, and `lix.sql.coherent_read_batch`; repository binding is `lix.repository.opened`; cold open is `lix.engine.open` plus `lix.session.open`. Writes expose additive `lix.transaction.materialize`, `lix.transaction.storage`, and `lix.transaction.notify` phases with a shared `lix.commit_cohort_id`. Callback spans use schema v2 and carry trace, span, and parent IDs. Fine-grained `lix_perf` diagnostics remain DEBUG-only.
