# HTTP and shared-replica admission

The browser runtime has one internal HTTP request ABI: `{url, init, response}`. `response` is either `{mode: "buffered", maxBytes}` or `{mode: "streaming"}`. Sync WASM, remote WASM, admission and worker RPC use this same response-policy union. Request cancellation crosses RPC explicitly; disposing a port cancels its requests and retained stream readers. There are no private properties on `RequestInit`.

A public `server.fetch` callback remains an ordinary network/telemetry adapter. It must preserve explicit credential headers. Delegate actual I/O to exported `networkFetch`: it validates request arguments before native fetch and classifies native rejection at the I/O boundary. An arbitrary callback's `TypeError` is a callback failure, not evidence of network unavailability. Neither opaque cookies nor invisible identity substitution are an admission mechanism. Shared-owner requests omit ambient cookies.

Finite remote SQL protocol responses have a 16 MiB response budget; observations and snapshots use streaming. Admission uses a 16 KiB budget and ten-second cancellation deadline. Response limits cancel the body; worker disposal also aborts active fetches. Contract, network, callback, response-budget, HTTP authorization, protocol/epoch, cancellation and owner-loss failures remain distinct. Worker serialization retains at most three nested causes with bounded, credential-redacted diagnostics.

## Metadata operation

`GET /lix/v1/{repository UUID}/admission` (normalized from the public `/lix/{UUID}` locator), with `lix-sync-protocol-version: 16`, returns authenticated metadata:

```
{ repositoryId, principalId, protocolEpoch: 16, storageEpoch: 81 }
```

The gateway authenticates and authorizes the request. The reference host reads validated durable catalog metadata; this operation never opens an engine, creates a SQL session, starts synchronization or migrates storage. Principal IDs follow the host's bounded opaque-account contract (1–255 visible ASCII characters), rather than imposing a new UUID requirement. The stable repository ID must match the requested repository. Unsupported/missing epoch metadata requires explicit migration.

## Ownership and credentials

The SharedWorker owns one logical engine; the dedicated OPFS worker retains physical SQLite ownership. SharedWorker names include the protocol/storage epochs. Its lifecycle is closed, opening, ready, closing or migration-exclusive. A failed close keeps ownership and rejects new attachments until closing succeeds. SharedWorker disconnection is acknowledged only after session and root closure; callers receive a typed failure if physical-release completion cannot be confirmed within ten seconds.

Admission precedes storage opening. Initial native requests use the exact admitted credential snapshot while opening is frozen. The owner commits its principal only after validating the opened root's account; a failed first opener cannot leave a provisional principal installed. Port sessions remain independent of the remote transport lease.

On refresh, candidate credentials must pass metadata admission before any remote request uses them. A different principal cannot replace the owner's identity. Before sending a request, the owner can select another live port if one cannot verify its candidate credentials. It never automatically replays a request after sending it through another port: operation-level idempotency and completion ownership remain with the engine.

Offline policy:

- Existing admitted local sessions and their pending edits survive refresh or remote authorization failures. Unverified credentials are never installed as a remote lease.
- New unseen credentials fail with `LIX_IDENTITY_UNVERIFIED_OFFLINE`.
- An exact credential proof held only in the same worker may authorize cached local attachment on actual network failure, when repository, account and epochs match. That proof does not authorize remote I/O, including while reopening a root.
- HTTP denial, contract failure, callback error, cancellation and epoch mismatch never trigger offline fallback. Proofs are bounded and never persisted.

## Qualification

`src/http-transport.test.ts`, worker tests and remote tests cover request composition, bounded responses, classified errors, principal changes, initial-open failure, close failure, callback/credential pairing, and actual generated remote WASM.

`admission-regression.browser.config.ts` runs the actual SharedWorker and HTTP/socket-disconnect path with a local-engine fixture, including exact-token offline attachment and dynamic refresh.

The separate full-composition gate runs the real reference authority, generated WASM, SharedWorker and OPFS:

```
cargo test -p lix-server composed_browser_shared_admission_and_opfs -- --ignored --nocapture
```

It requires built SDK/OPFS browser artifacts and Chromium. Its explicit launcher owns the temporary authority lifecycle; generic browser runs do not silently substitute a fake authority.
