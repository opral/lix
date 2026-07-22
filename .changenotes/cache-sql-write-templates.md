---
type: patch
---

Repeated SQL writes now reuse parsed and bound templates plus stable catalog
metadata. Transactions derive SQL-visible schemas from their compiled opening
catalog instead of rereading durable schema state, while every execution still
builds fresh snapshot-specific DataFusion providers and plans.
