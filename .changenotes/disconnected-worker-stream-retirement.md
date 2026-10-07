---
type: patch
---

The JavaScript SDK now fails every active HTTP response stream when its worker host disconnects, including backpressured streams with queued bytes and no pending pull. This prevents callers from consuming stale queued responses after disconnection.
