---
type: minor
---

Every commit now publishes an immutable tracked-state root, making historical diff and merge scale with changed state instead of replayed history. Repositories using the previous rootless-history protocol are rejected and must be recreated.
