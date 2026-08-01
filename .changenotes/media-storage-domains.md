---
type: patch
---

Media file transfers now use durable atomic publication, raw immutable SlateDB payload segments, bounded HTTP response bodies, and ordinal CAS range reads without duplicating temporary upload metadata.
