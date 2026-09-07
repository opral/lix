---
type: patch
---

Fixed SQL filters missing rows inserted or updated within the same transaction.

Queries, including CTEs, now evaluate predicates against staged values consistently. Updates and deletes that select rows by those values also see earlier writes in the transaction.

Branch deletion now respects default-branch changes made in the same transaction, and plugin file materialization recognizes staged binary blob references without losing their durable proof.
