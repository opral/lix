---
type: minor
---

Excalidraw scenes now expose root metadata as `scene_json`. Element type and deletion state are edited directly in `element_json`; duplicate `element_type` and `is_deleted` columns are removed. Formatting hints are optional for SQL inserts, and rows with equal order keys sort by native ID.

Excalidraw preserves durable ordering, numeric spelling, collection layout, and unknown data across file edits, SQL updates, and cold restoration. Scene templates cannot override row content. Warm element SQL edits and grouped file edits use sparse indexed reads; paged indexes and offset rebuilding have scaling regressions and compiled SQL/Wasm coverage.
