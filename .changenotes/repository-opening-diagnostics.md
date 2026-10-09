---
type: patch
---

Repository opening now reports actual migration progress and stalled dependencies, even while an open operation remains unfinished.

Request deadlines are distinguished from capacity exhaustion and migration stalls. Retries share the existing opener, preserving migration ownership and stored data.
