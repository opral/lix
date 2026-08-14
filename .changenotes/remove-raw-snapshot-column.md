---
type: minor
---

Row tables and row history no longer expose the raw `lixcol_snapshot_content` column.

Select the registered schema's typed columns instead — they are the same data, already parsed, and they no longer force every reader to decode a whole JSON snapshot to reach one field. History tombstones remain observable through `lixcol_is_deleted`. Queries that still select `lixcol_snapshot_content` now fail with a column-not-found error.
