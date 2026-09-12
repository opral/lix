---
type: patch
---

OPFS partial replicas now share loaded data and offline edits across browser tabs.

Tabs coordinate one engine automatically through a SharedWorker, keeping the same
storage identity and `openLix()` API. Closing a tab leaves the other sessions
operational. Checkpoint history also fetches missing commit metadata on demand
and retains it for subsequent offline reads.

File checkpoints upload their blob dependencies before publishing, including
after an offline checkpoint or a lost acknowledgment. Each browser session keeps
its own telemetry callback and trace parent; shared background spans go to live
subscribers.

Pending edits and checkpoint dependencies upload in bounded waves, including
recovery after lost replies. SQL retries preserve the completion boundary: an
error reported after execution or commit cannot replay the operation.

Read-interest journal flushing retries expired snapshots internally, so concurrent
tab startup can complete without replaying the SQL that registered its inputs.
