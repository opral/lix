---
type: patch
---

Server SQL mutations now store much smaller retry receipts.

Every `/execute` write records its response so a retry with the same
`Idempotency-Key` returns the recorded result instead of mutating twice. That
record previously encoded returned binary values as arrays of decimal numbers,
costing about 3.6 stored bytes per payload byte, so a mutation using `RETURNING`
over file content retained nearly four permanent copies of it. Receipts now use
a compact encoding: about 1.33 bytes per payload byte and about a third less for
mutations with no `RETURNING`. The 8 MiB replay limit consequently accepts
roughly 6 MiB of returned content where it previously accepted about 2.2 MiB.
