---
type: patch
---

Improved remote active branch reads by reusing the session state established
during the initial handshake.

`activeBranchId()` now avoids a redundant network request and remains
synchronized after successful branch switches. Ambiguous switch failures
invalidate the cached value so the next read reconciles it with the server.
