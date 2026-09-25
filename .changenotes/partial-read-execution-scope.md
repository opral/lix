---
type: patch
---

Partial replicas execute read-only SQL on the selected authority branch when the local planner cannot prove a complete result. Joins and read-only batches now use the same route as standalone history reads. Authority results report that unpublished local edits are excluded.
