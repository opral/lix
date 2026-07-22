---
type: patch
---

Avoid scanning every committed owner of a unique value when a single existing
entity keeps that value unchanged.

With the namespace shortcut already applied, SlateDB changed-metadata file
overwrite p50 improved from 7.767 ms to 5.766 ms with 100 files (25.8%), from
22.077 ms to 6.550 ms with 1,000 files (70.3%), and from 171.892 ms to 9.509 ms
with 10,000 files (94.5%).
