---
type: minor
---

Explicit transactions now conflict only when a concurrent commit touched what they read or write, and `lix.transaction()` reruns a callback on conflict.

Previously any concurrent write to the branch — even to an unrelated row — made `commit()` fail with `LIX_TRANSACTION_CONFLICT`. Lix now re-checks the rows that the transaction's explicit reads and `UPDATE`/`DELETE` predicates depended on, plus the rows it writes (per file for plugin-backed file content), and rebases the commit onto unrelated concurrent changes. Real conflicts still fail, and their error details list the overlapping rows. The new `lix.transaction(fn, { maxRetries })` helper rolls back and reruns `fn` on a fresh transaction after such a conflict.
