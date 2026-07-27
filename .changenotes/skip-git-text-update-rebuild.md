---
type: patch
---

Speed up Git-text file updates by retaining the validated splice result and
construction-ordered line table instead of sorting, rendering, and validating
the complete successor document again.
