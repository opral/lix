---
type: minor
---

Reading JSON columns no longer rebuilds a JSON document per row on every scan.

Lix already stores JSON in a single canonical form, so query results now carry
those exact bytes instead of parsing each cell into an intermediate JSON
document and immediately re-serializing it. Full-table reads of JSON-bearing
tables get meaningfully faster, and results are byte-identical to before. In
the Rust API, `Value::Json` now holds a `lix::Json` value; call
`Json::to_value()` where a `serde_json::Value` is needed.
