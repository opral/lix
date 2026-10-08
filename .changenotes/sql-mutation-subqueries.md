---
type: patch
---

Fixed UPDATE and DELETE predicates containing subqueries, allowing applications to mutate related rows directly in SQL. Mutations also support target aliases and preserve OLD and NEW row values in RETURNING expressions.
