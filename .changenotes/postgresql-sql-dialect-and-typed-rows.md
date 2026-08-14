---
type: minor
---

Lix SQL now uses the PostgreSQL dialect.

Queries use PostgreSQL syntax and numbered parameters such as `$1`. Row tables expose native SQL types, including `jsonb` and `timestamptz`, with consistent row terminology and typed columns instead of raw snapshots.
