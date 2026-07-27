---
type: patch
---

Adds the Git-compatible Component-v2 text plugin. It selects files with Git's
bounded 8 KiB NUL-byte predicate and represents matching payloads as lossless,
stable semantic line rows while leaving NUL-bearing files on the raw binary
path.
