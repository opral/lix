---
type: patch
---

Filesystem sync now reports symlinks and other unsupported entries that block a regular Lix file instead of silently leaving Lix and disk out of sync.

Git replay now also rejects unsupported paths and entries explicitly instead of representing them as regular files.
