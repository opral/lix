---
type: patch
---

Treat disappearance during retired disk-cache deletion as successful cleanup. Keep other I/O errors and unsafe entry types terminal. This prevents already-deleted nested cache directories from retaining failed cleanup capacity and blocking new repository opens.
