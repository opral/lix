---
type: minor
---

Remote clients can now open an immutable, server-owned branch-pinned session
with `server.branchId`.

Pinned operations avoid the shared workspace branch-selector lookup and cannot
be moved by another client's branch switch. The canonical protocol exposes the
same mode through the initial `GET /lix/v1?branchId=<id>` handshake and reports
the authoritative `sessionScope` in handshake responses.
