---
type: minor
---

Changed `lix.clientState.get()` to read asynchronously from the configured client storage.

Client-state reads now use the authoritative Lix transaction path instead of an eagerly hydrated in-memory copy. Callers must await `get()`.
