---
type: minor
---

Return one durable commit receipt per SQL transaction.

`executeBatch` now returns `{ results, commit }`, replacing the array of results
with repeated commit spans. Explicit transaction `commit()` returns `{ commit }`;
its statement results carry no receipt. Server protocol 9 carries these contracts
through HTTP, native bindings, browser workers, and the JavaScript SDK.

Explicit SQL reads used to decide later writes now fence the transaction's
opening branch snapshot. Concurrent branch changes require retrying the complete
transaction, including application checks. Read-only transactions remain valid.
Known durable completion errors preserve the receipt in `details.commit` and
remain forbidden from automatic mutation retries.

Explicit SQL transactions publish individually to preserve exact receipts. They
no longer combine multiple transactions into one merged commit; concurrent
commits still use the coordinator and retain stale-write reconciliation.
