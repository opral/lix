---
type: patch
---

Fixed concurrent cold starts of Lix in multiple browser tabs.

Browser workers now coordinate the initial load of the fingerprinted Lix WebAssembly asset so every tab opens reliably while retaining immutable asset caching.
