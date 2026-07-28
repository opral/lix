---
type: minor
---

Lix SQL and history are more capable and easier to use.

History queries now default to the active branch head and correctly reconstruct
files and directories across merges. The public catalog is smaller,
`information_schema.columns` is the authoritative type contract, and the SDK
adds atomic SQL batches alongside `DELETE ... RETURNING`, `LIKE` and `ILIKE`,
and binary casts.

Applications using the removed generic state tables, low-level filesystem
tables, or former filesystem-history provenance columns must migrate to the
typed schema, logical file, and `lixcol_source_changes` surfaces.
