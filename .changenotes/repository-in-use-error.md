---
type: patch
---

Filesystem repository opens now report `LIX_STORAGE_IN_USE` when another process
holds the repository lock. Applications can distinguish ownership contention
from storage failures without matching diagnostic text.
