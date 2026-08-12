---
type: minor
---

Added `IndexedDbStorage` as the browser persistence backend for `openLix`.

Browser applications can now persist complete local repositories, or private client state for remote sessions, through the same transactional storage API. The separate snapshot-storage API and its localStorage adapter have been removed.
