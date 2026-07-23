---
type: minor
---

Changed SlateDB storage to a versioned LZ4 physical layout.

Existing SlateDB-backed Lixes must be recreated. The new layout does not read
the previous physical namespace and provides no migration or compatibility
fallback.
