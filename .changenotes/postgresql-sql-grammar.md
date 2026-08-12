---
type: minor
---

Lix SQL now uses PostgreSQL grammar.

Queries use PostgreSQL-style numbered parameters such as `$1` and `$2`.
Anonymous `?` parameters are no longer accepted, and `VALUES` relations use
the PostgreSQL form `FROM (VALUES (...)) AS alias(column)`.
