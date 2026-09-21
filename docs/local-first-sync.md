# Partial replica with on-demand sync

JavaScript clients select `server.mode: "partial_replica"` and provide durable storage. Rust uses the storage-plus-server typed builder. Opening installs bounded metadata. SQL fetches missing native rows, ranges, schemas or content and retains them locally; unknown data is never treated as an empty result.

## Local reads and writes

Covered reads execute locally. Writes whose read, validation and publication dependencies are resident commit after local durability, without waiting for the server. Call `lix.execute()` with the intended SELECT on hover to fetch its inputs ahead of interaction. Writes use ordinary `execute()`; a cold write may fetch additional validation or commit inputs and can fail offline. One successful query does not prepare every possible subsequent statement.

Current data means the coherently synchronized local state plus pending local writes. Immediate reads see those writes. Background synchronization publishes coherent branch coordinates without first warming every retained query. Queries and observers hydrate missing inputs against the new state, including newly matching rows and rows leaving a scope. A cached query may need network data after a remote update. Own acknowledgments preserve the local working set and newer local commits.

## Reconciliation and migration

Immutable native commits and durable branch references form the upload outbox. Retries preserve commit identity and authority admission coordinates. Native reconciliation validates the compatible baseline and local pending work before publication. An unavailable baseline or unsupported operation must not silently reset pending edits or switch to remote execution. See the [migration guide](./partial-replica-migration.md) for conversion, retained sources, recovery-required cases and supported repository formats.

Upgrade the SDK and authority together. Existing full replicas require explicit conversion. Migration is separate from bounded opening and can require time, storage and network access. Missing or unsupported repository formats fail explicitly; opening never falls back to full bootstrap.

The optional server SQL result fallback is not implemented. Cold SQL hydrates native inputs; unsupported inventories fail explicitly. Aggregate result caches are not a substitute for native coverage or editable rows.

See [configuration](./collaboration-and-sync.md), [on-demand hydration and limits](./partial-replica-on-demand-sync.md), and [performance evidence](./partial-replica-performance.md).

### Transient authority admission failures

Opening a browser replica retries the read-only admission GET on network errors
and HTTP 502, 503, or 504. There are at most five attempts with five-second request
timeouts and 0.5, 1, 2, and 4-second backoffs. Authentication, compatibility,
malformed-response, and cancellation errors remain terminal. Typed migration
waits retain their separate caller-cancellable policy.

An exhausted network failure retains its network error code so verified cached
admission can still open offline. Error details include `admissionRetryExhausted`
so applications can offer manual Retry without automatically restarting the SDK's
budget. Admission retries never replay SQL or clear local storage.
