# Sync module boundary

The synchronization subsystem has three layers:

- `mod.rs` contains the shared durable sync engine: wire/state models,
  outbox/canonical application, lazy hydration, and branch reconciliation.
- `runtime.rs` is the single lifecycle state machine for bootstrap, reconnect,
  long-poll orchestration, outbox admission, retry/backoff, and shutdown.
- `platform.rs` and `platform/` are the only target-dependent layer. Native
  uses Tokio and reqwest. Browser WASM uses `spawn_local`, browser timers,
  fetch, and `AbortController`.

`contract.rs` is the platform-neutral transport interface between the shared
engine and the platform HTTP adapters. Target `cfg`, HTTP libraries, and task
spawning must not leak into `mod.rs` or `runtime.rs`; a compile-time source
boundary test in `platform.rs` enforces that rule.

Lifecycle sync always uses the server's cancellable event long poll. The
interval-based `SyncClient::run_polling_until` API remains only as a backwards-
compatible helper for explicit/manual clients and is not used by server sync
mode.
