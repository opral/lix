---
type: patch
---

Added a vendor-neutral `lix.repository.opened` engine span when a client session binds to a Lix.

In-process `open_lix()` and protocol handshake session creation each emit one span. Protocol servers opened with `open_lix().serve()` attach their telemetry sink while creating no application session and emitting no opened span. Hosts that mint a session against an already-open runtime call `Lix::bind_session()` or `lix::bind_session`.
