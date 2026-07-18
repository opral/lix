---
type: minor
---

Remote-only browser deployments can now exclude the local Lix worker and engine WASM.

Import `openLix` from `@lix-js/sdk/remote` to keep the thin-client bundle independent of the local engine while using the same Lix facade.
