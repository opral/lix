---
type: patch
---

Made file deletion substantially faster by cascading file-scoped state while materializing current state instead of recording a separate historical deletion for every file-owned entity.
