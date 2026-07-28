---
type: minor
---

Plugin-backed atomic imports now scale independently of document count. The
engine automatically reuses its bounded live-Store working set for fresh
documents while retaining existing-document leases through commit, so callers
no longer need a special single-writer ingestion API or actor-retention policy.
