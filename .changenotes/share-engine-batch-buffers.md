---
type: patch
---
Move SQL and plugin-backed mutations onto shared typed batches, retaining canonical JSON, identifiers, and encoded storage buffers once per batch instead of cloning them per row.

Read, diff, and merge materialization now retain shared payload buffers through conflict planning and transaction staging.
