---
type: patch
---

Fixed remote observations failing when an application opens more than 32 live queries.

The remote client now keeps each multiplex stream within the server's safety limit while transparently distributing additional observations across coordinated streams.
