---
type: minor
---

Lix SQL file writes now support explicit casts to binary data.

Use `CAST(value AS BINARY)` when inserting or updating UTF-8 text in `lix_file.data`.
