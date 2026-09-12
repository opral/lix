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
