---
type: minor
---

Remote Lix clients now support live query observations.

`lix.observe()` streams server-side Lix results, reconnects transient failures, follows successful branch switches, and closes with the normal Lix lifecycle.
