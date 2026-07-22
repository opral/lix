---
type: minor
---

Remote Lix clients now use independent server-issued workspace sessions.

The initial protocol handshake returns an opaque session identifier that is required on subsequent SQL, branch, and observation requests. Unknown or expired sessions fail closed so a client must reload before writing from a stale view.
