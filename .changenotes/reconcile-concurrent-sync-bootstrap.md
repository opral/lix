---
type: minor
---

Centralized first-time sync admission around one durable replica binding and one atomic snapshot installer. Simultaneous opens now restart through the existing whole-open retry path until one publication is durable; aliases cannot claim the same local replica.

Hard cut: JavaScript sync calls must now pass `storage` (`openLix({ storage, server: { mode: "sync", ... } })`). The implicit volatile Memory fallback no longer type-checks and is rejected at runtime because Memory cannot prove durable bootstrap publication.
