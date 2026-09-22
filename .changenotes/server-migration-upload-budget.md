---
type: patch
---

Allow bounded S3 uploads of large immutable segments during repository migration to take longer than interactive cache reads. Preserve the short read timeout and conditional-write semantics.
