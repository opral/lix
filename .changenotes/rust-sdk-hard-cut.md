---
type: minor
---

Unified the Rust engine and SDK as the `lix` crate, with `open_lix().await?` as the in-memory quick start and builder methods for storage, telemetry, and custom Wasm runtimes.

This is a breaking Rust API migration: `lix_engine`, `lix_sdk`, `OpenLixOptions`, and the specialized `open_lix_with_*` entry points have been removed. Persistent backends now live in independently versioned `lix-storage-*` crates.
