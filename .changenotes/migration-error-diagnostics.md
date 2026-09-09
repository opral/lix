---
type: patch
---

Server migration failures now return a safe diagnostic message and the originating migration error code when available, including failures returned to later callers. Copy destination precondition conflicts are identified explicitly; other failures direct operators to the full server logs without exposing storage credentials. HTTP status codes and retry behavior are unchanged.
