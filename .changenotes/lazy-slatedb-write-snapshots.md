---
type: patch
---

SlateDB-backed writes now create a read snapshot only when a transaction deletes a range.

Ordinary puts and point deletes avoid the extra worker round trip while range deletes retain a stable base snapshot.
