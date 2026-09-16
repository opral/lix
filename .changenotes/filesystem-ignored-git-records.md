---
type: patch
---

Filesystem synchronization preserves `.git` files stored in Lix during live disk reconciliation. These paths remain excluded from disk materialization; their intentional absence on disk no longer deletes the stored records during the session. Reopening still cleans up previously imported Git entries.
