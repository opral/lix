---
type: patch
---

Large delete transactions now index pending tombstone identities instead of
repeatedly scanning every deleted row during validation.
