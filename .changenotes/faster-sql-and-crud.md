---
type: patch
---

SQL queries and everyday CRUD operations are substantially faster.

Lix now reuses SQL sessions and prepared plans, seeks directly for indexed and file-scoped lookups, and avoids unnecessary intermediate materialization when returning typed and JSON results.
