---
type: patch
---

Deleting hosted repositories keeps other requests responsive. Protocol close now releases its background owners before reporting completion, keeping final storage teardown with the host that owns it.
