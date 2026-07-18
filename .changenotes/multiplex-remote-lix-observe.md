---
type: patch
---

Remote Lix clients now multiplex live query observations over one server stream.

Multiple `lix.observe()` calls no longer consume one HTTP connection each, so thin clients can keep executing queries while many observations are active.
