---
type: minor
---

Remote Lix clients now use independent, branch-pinned server sessions.

The initial protocol handshake returns an opaque session identifier that is required on subsequent SQL, branch, and observation requests. Unknown or expired sessions fail closed so a client must reload before writing from a stale view.

Switching branches changes only that client session, so one client can no longer
change the active branch observed by another client.
