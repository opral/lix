---
type: patch
---

Filesystem sync now reports symlinks and other unsupported entries that block a regular Lix file instead of silently leaving Lix and disk out of sync.

Lix file paths remain literal UTF-8, and experimental Git replay now rejects non-UTF-8 paths, symbolic links, and gitlinks rather than encoding or representing them as regular files.
