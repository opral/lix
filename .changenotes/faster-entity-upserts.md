---
type: patch
---

Sped up `INSERT ... ON CONFLICT` entity upserts by scanning only the inserted identity for conflicts instead of the full entity state.
