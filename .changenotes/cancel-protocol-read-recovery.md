---
type: patch
---

Cancelled read-only protocol requests now release their session locks so a
timed-out workspace can close and recover promptly. Writes and durable runtime
functions still complete after a client disconnects.
