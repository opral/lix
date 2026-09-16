---
type: patch
---

Filesystem mirror updates now replace each file atomically, so readers no longer
observe partially written contents. This does not add a power-loss durability
guarantee for mirrored files or make multi-file updates atomic.
