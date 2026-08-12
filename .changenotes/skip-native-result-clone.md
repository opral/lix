---
type: patch
---

Faster JSON-heavy SQL reads by removing a redundant native-result copy.

Native query results are already materialized for the result set, so the SDK no longer deep-clones structured values while wrapping them. Public `toJS()` and row accessors still return defensive copies.
