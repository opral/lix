---
type: patch
---

Reduced SlateDB open latency and object-store traffic by loading cached SST data
on demand instead of preloading live SSTs up to the disk-cache limit.
