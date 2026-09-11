---
type: minor
---

Write results now report the commits they moved the active branch between.

`execute` and `executeBatch` results carry `commit: { before, after }` for every statement that committed a write: `before` is the active branch's head before the write and `after` the head it published. `lix_diff('lix_file', before, after)` is then exactly what the write changed, with no readback. Every statement of a written batch carries the batch's one span, and a write that published no commit on the active branch reports both ids equal. Read statements outside a written batch, read-only batches, and statements inside an explicit transaction carry no span. The server protocol, the remote client, and the CLI's JSON output pass it through, and results from servers that predate it simply omit it.
