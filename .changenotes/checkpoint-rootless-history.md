---
type: patch
---

Checkpoint commits now retain compact absolute deltas instead of rewriting a full immutable history root.
Git replay can checkpoint at a configured commit interval and records checkpoint latency in its profile.
Repository GC checkpoints now double their sweep interval so cumulative maintenance stays linear in history.
