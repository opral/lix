---
type: minor
---

Plugin-backed atomic imports now scale independently of document count. The
engine automatically reuses its bounded live-Store working set for fresh and
existing documents while preserving actively contested same-file leases, so
callers no longer need a special single-writer ingestion API or
actor-retention policy. Retained session observations also recover from benign
working-set eviction when their exact durable semantic root is unchanged.
