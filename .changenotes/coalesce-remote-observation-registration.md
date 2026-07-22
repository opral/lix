---
type: patch
---

Reduced remote observation startup work when an interface registers several queries together.

Same-turn registrations now open one multiplex stream with the complete subscription set instead of repeatedly opening and aborting partial streams.
