---
type: patch
---

Local and remote JavaScript SDK handles now share Wasm initialization, preventing concurrent opens from initializing the same module twice.
