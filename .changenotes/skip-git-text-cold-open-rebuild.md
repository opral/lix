---
type: patch
---

Speed up Git-text cold opens by retaining the already validated source buffer
and construction-ordered line table instead of validating, sorting, rendering,
and validating them again.
