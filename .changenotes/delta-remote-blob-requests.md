---
type: patch
---

Reduced remote sync latency and bandwidth for localized edits to large files.

Remote clients now transparently send compact byte splices after a successful full write, while automatically retrying with full bytes if the server no longer has the required base.
