---
type: patch
---

Fixed SQL filters missing rows inserted or updated within the same transaction.

Queries, including CTEs, now evaluate predicates against staged values consistently. Updates and deletes that select rows by those values also see earlier writes in the transaction.
