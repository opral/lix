---
type: patch
---

Filesystem sync now reports symlinks and other unsupported entries that block a regular Lix file instead of silently leaving Lix and disk out of sync.

The engine now owns and enforces the literal UTF-8 logical-path contract, including descriptor names and NUL rejection. Experimental Git replay rejects non-UTF-8 paths, symbolic links, and gitlinks rather than encoding or representing them as regular files.
