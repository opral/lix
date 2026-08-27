---
type: minor
---

Added opaque row references and a single SQL checkpoint function for full and scoped checkpoints.

Diff and selection surfaces now use `row_ref`, scoped checkpoints accept arrays of row references, and omitted diff commits default to the latest checkpoint through the active branch head. The former typed checkpoint SDK and two-column JSON row-key selection contract have been removed.
