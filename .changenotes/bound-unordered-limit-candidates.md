---
type: patch
---

Unordered small LIMIT scans can read a bounded set of authenticated current-base keys and recheck their current visibility, avoiding full row materialization when enough live candidates are available. Unsupported layouts and insufficient candidate pages retain the regular scan.
