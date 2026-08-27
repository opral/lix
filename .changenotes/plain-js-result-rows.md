---
type: minor
---

Hard-cut JavaScript SQL results to plain-object rows and typed column descriptors. `execute`, `executeBatch`, transactions, and observations now return enumerable rows with direct property access and `columns` entries shaped as `{ name, type }`; the `Row` accessor API has been removed. Positional array rows remain available through `rowMode: "array"` for duplicate-column and wire-adapter use cases. The Lix Server Protocol is now version 5.
