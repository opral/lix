---
type: patch
---

Full scans of supported packed collections now project bounded batches of eight authenticated mutation parts, with bounded HOT and global overlays. This avoids retaining every row and payload in memory before SQL execution and shares reads of physical storage extents. Small collections and unsupported layouts keep the existing scan path, and SQL ordering remains explicit.
