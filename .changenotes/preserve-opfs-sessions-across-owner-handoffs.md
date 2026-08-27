---
type: patch
---

OPFS repositories remain writable across browser-tab navigation and owner-worker handoffs.

The shared storage session now survives an OPFS backend restart, so one healthy tab no longer fences another tab using the same repository generation.
