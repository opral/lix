---
type: patch
---

Production traces now show the real commit, storage, notify, checkpoint, and session-open phases.

The phases that can take tens of milliseconds after a SQL batch already existed as debug-only `lix_perf` spans. They now emit at the same INFO `lix_sql` / `lix` plane as `SQL batch` and `lix.opened`, so a write batch can split materialize vs storage flush vs notify, checkpoint/create carries `commit_id`, and a cold open shows engine and session construction instead of only the instantaneous bind.
