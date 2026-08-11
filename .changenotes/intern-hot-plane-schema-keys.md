---
type: patch
---

Shrunk the serving-plane storage keys by interning schema keys.

Live-state row, file-membership, working-diff, collection-control, and
packed-base keys now carry a fixed 4-byte schema id instead of the repeated
escaped schema-key string. The id mapping lives in one tiny append-only
storage space and is published atomically with the first row that uses it.
