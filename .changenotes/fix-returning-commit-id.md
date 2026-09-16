---
type: patch
---

Fixed repeated row replacement within SQL batches and explicit transactions.

Repeated updates now retain the same transaction commit ID, so earlier `RETURNING lixcol_commit_id` results agree with the published commit after the transaction succeeds. Deleting and reinserting the same key within a transaction no longer reports a false duplicate-key error; live duplicate keys remain rejected.
