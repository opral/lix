---
type: patch
---

Improved large binary file write performance on native storage.

Lix now checks compact content-presence markers when deduplicating binary chunks, avoiding reads of unchanged chunk payloads during common localized file updates.
