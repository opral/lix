---
type: patch
---

Speed up experimental Git replay by batching bounded persistent `git cat-file` blob requests.

The reader flushes up to four object requests at once, then fully drains their ordered responses before sending the next window. This removes per-object pipe round trips without risking large-blob backpressure deadlocks.
