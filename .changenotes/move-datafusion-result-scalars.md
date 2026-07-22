---
type: patch
---

Reduced SQL result materialization overhead by moving owned text and blob values out of DataFusion instead of copying them into Lix values.
