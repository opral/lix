---
type: patch
---

Use the workspace mimalloc allocator in the native server, matching the native CLI, to reduce allocation overhead and retained memory during repeated large scans.
