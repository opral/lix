---
type: minor
---

Removed `lix.clientState` and remote client-storage composition.

Applications now own browser-local UI persistence explicitly, while remote Lix handles remain focused on repository operations and independent branch-pinned sessions.
