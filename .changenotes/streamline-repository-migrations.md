---
type: patch
---

Reduce repository upgrade I/O by skipping obsolete derived indexes, clearing candidate spaces in one durable transaction, and reusing the catalog record during authority opening.
