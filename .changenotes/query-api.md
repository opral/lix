---
type: minor
---

Lix has a new SQL API: `query()` for reads, `execute()` for any statement, `executeMany()` for one statement over many parameter sets, `stream()` for large reads, and snapshots for several reads at one state.

`query()`, `queryOne()` and `stream()` never write: a write rejects with `LIX_ERROR_READ_ONLY`. `execute()` runs writes, DDL and reads and returns rows, `rowsAffected` and the commit it published. Buffered results are capped at 64 MiB of result memory (`maxBytes`, `Infinity` opts out) and fail with `LIX_ERROR_RESULT_TOO_LARGE` instead of exhausting memory. `stream()` reads any result with bounded memory. `lix.read(snapshot => ...)` and `lix.snapshot()` run several reads against one state. Reads have no deadline: aborting a read's `signal`, closing its snapshot or closing the Lix stops the engine work. `executeBatch`, `rowMode` and the coherent-read helpers are removed; run several statements atomically with `lix.transaction()`. In Rust, `lix.query(sql)` and `lix.execute(sql)` are builders with `.params()`, `.all()`, `.one()`, `.optional()` and `.stream()`.

Error codes introduced with this API follow the `LIX_ERROR_*` naming: `LIX_ERROR_RESULT_TOO_LARGE`, `LIX_ERROR_NOT_UNIQUE`, `LIX_ERROR_NO_ROWS`, `LIX_ERROR_AMBIGUOUS_COLUMN`, `LIX_ERROR_SNAPSHOT_CLOSED`, `LIX_ERROR_SYNC_TRANSPORT_STALLED` and `LIX_ERROR_CANCELLED` (earlier development builds used `LIX_RESULT_TOO_LARGE`, `LIX_NOT_UNIQUE`, `LIX_NO_ROWS`, `LIX_AMBIGUOUS_COLUMN`, `LIX_SNAPSHOT_CLOSED` and `LIX_SYNC_TRANSPORT_STALLED`).
