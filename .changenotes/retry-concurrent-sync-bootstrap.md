---
type: patch
---

Fixed simultaneous first-time sync opens against shared browser storage.

When another tab installs the initial server snapshot first, Lix now restarts repository open against that durable replica instead of rejecting the second tab.
