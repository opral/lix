---
type: patch
---

Observer watchers now stop when their last subscriber closes and release their storage handles when the owning engine is dropped. Native shutdown cancels and joins the watcher instead of leaving a detached polling thread running. Snapshot cleanup and checkpoint garbage collection release their task-owned storage handles before reporting completion.
