---
type: patch
---

Filesystem synchronization preserves `.git` files stored in Lix when reconciling disk changes. These paths remain excluded from disk materialization; their intentional absence on disk no longer deletes the stored records.
