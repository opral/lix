---
type: major
---

Plugin packages now have explicit validation and resource limits.

Malformed ZIP packages and manifests, including invalid path globs, now fail with
`LIX_ERROR_INVALID_PLUGIN`. Packages are limited to 32 MiB of archive bytes,
128 ZIP entries, 64 MiB declared expansion, 32 MiB expanded per entry, 64 KiB
manifests, 1 MiB per schema, 64 declared schemas, 512-byte paths, and
1,024-character globs. ZIP parsing accepts stored or deflated entries, comments,
data descriptors, and bounded per-entry ZIP64 sizes, but rejects ZIP64 central
directories and archives with more than eight complete footer candidates.
