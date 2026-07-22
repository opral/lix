---
type: patch
---

Improved metadata-only file updates by checking the affected descriptor directly
instead of scanning the entire workspace namespace when its path is unchanged.

Process-median SlateDB p50 improved from 36.639 ms to 23.209 ms with 1,000
files (36.7%) and from 317.733 ms to 156.995 ms with 10,000 files (50.6%).
