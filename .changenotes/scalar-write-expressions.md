---
type: patch
---

SQL updates now support scalar expressions such as `replace`, `concat`, `coalesce`, and string concatenation in file and row mutations.

Assignments, filters, and returned expressions use the same scalar function rules as reads. Invalid expressions reject the mutation atomically. Partial replicas can use resident file content in an exact-path expression update while offline.
