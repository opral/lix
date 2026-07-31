---
type: minor
---

SQL writes now support `RETURNING` across registered entities and writable filesystem and branch surfaces. INSERT and UPDATE return final post-write values (including generated defaults), while DELETE continues to return the removed row values.
