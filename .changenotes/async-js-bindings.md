---
type: patch
---

Made the JavaScript SDK's native bindings fully asynchronous.

Awaited methods previously blocked the calling thread inside the native binding, which could freeze an Electron main process. Opening a lix, `execute`, transactions, branch and merge calls, observers, and `close` now return real promises and run their work off-thread.
