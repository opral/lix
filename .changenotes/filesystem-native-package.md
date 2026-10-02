---
type: minor
target: lix
---

Move Node.js native binaries from `@lix-js/sdk` to `@lix-js/storage-filesystem`.
The SDK uses WASM for memory and JavaScript storage sessions and no longer installs
native platform packages. Filesystem sessions retain the existing Rust engine,
RocksDB adapter, synchronization, exclusive locking, and migration support.
Install matching SDK/filesystem versions with optional dependencies enabled.
