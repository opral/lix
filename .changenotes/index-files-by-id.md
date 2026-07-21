---
type: patch
---

Common exact-ID `lix_file` selections now binary-search a collision-safe
secondary index when reusing the filesystem path cache, instead of scanning
every visible path entry.
