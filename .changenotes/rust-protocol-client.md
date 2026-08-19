---
type: patch
---

Remote `openLix({ server })` now uses one Rust Lix Server Protocol client.

JavaScript only supplies `fetch` and authentication headers. Session expiry (`LIX_ERROR_PROTOCOL_SESSION_GONE` and `LIX_ERROR_PROTOCOL_SERVER_CLOSED`) recovers once by opening a new session that pins the last known branch, then retries the original request.
