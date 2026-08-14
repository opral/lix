---
type: patch
---

x86_64 builds now require SSE4.2 and PCLMULQDQ, raising the baseline to roughly `x86-64-v2`.

These instructions let the storage engine compile its hardware CRC32C implementation instead of the software fallback, which speeds up reads of large binary files. Every x86_64 CPU since roughly 2009 (Intel Nehalem, AMD Bulldozer) provides them, but a build targeting an older baseline will no longer run. ARM builds are unaffected.
