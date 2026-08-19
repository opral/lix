---
type: minor
---

Removed the public SQL script-parsing API from the Rust and JavaScript SDKs.

Hosts no longer call `parse_sql_script` / `parseSqlScript`. `execute()` runs one statement. To run several statements atomically, pass an array of `{ sql, params? }` objects to `executeBatch()`.
