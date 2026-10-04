---
type: patch
---

Supported packed row scans now prune immutable mutation parts using primary-key bounds before loading payloads. Snapshot admission still checks the full collection and file scope, and range scans recheck typed bounds on current rows and overlays. Unsupported layouts retain the existing scan path.
