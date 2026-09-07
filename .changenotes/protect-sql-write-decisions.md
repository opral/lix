---
type: patch
---

Prevent concurrent SQL updates and deletes from silently committing stale decisions.

Transactions that plan an `UPDATE` or `DELETE` now reject an intervening change
to their active-branch or shared/global state with `LIX_TRANSACTION_CONFLICT`,
instead of merging the stale write. Retry an explicit transaction from the
beginning. Local `execute()` and `executeBatch()` retry conflicts automatically
within their existing limits.
`RETURNING` results inside an explicit transaction remain provisional until
commit succeeds. The branch-level check can also reject unrelated concurrent
edits; explicit branch merging remains available for collaborative changes.
