---
type: patch
---

Fixed partial replicas restarting remote dependency downloads when foreground reads interrupt synchronization.

Remote edits can finish syncing while local reads continue. Invalidated downloads release their remote and local resources before a replacement starts, without blocking resident reads.
