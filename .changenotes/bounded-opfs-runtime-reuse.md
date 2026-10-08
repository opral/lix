---
type: patch
---
Reuse a bounded warm worker runtime when reopening OPFS repositories. Close storage and release repository ownership before reusing an idle worker, preserve offline plugin readiness, and discard workers whose cleanup fails or times out.
