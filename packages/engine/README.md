# Lix Engine

The Lix engine is the portable core for Lix repository semantics. It should be
able to run anywhere Lix runs: native apps, servers, tests, JavaScript runtimes,
WASM hosts, and future embedded environments.

## Boundary

The engine owns:

- logical Lix state, schemas, transactions, branches, commits, and queries
- logical Lix paths such as `/docs/readme.md`
- binary CAS data after bytes have been imported into Lix
- the storage backend contract for ordered key/value reads and writes

Direct host system access is out of scope for engine logic. In particular,
engine code should not depend on:

- host filesystem paths for user files
- directory walking or file materialization
- filesystem watchers or polling
- symlink, case-sensitivity, permissions, or platform path policy
- OS-specific storage locations or folder layout

Those responsibilities belong in backend or SDK adapters, such as `FsBackend`.
Adapters may map host resources into logical Lix paths, hydrate bytes on demand,
watch external changes, and materialize writes. Host resources enter the engine
through narrow backend capabilities, such as `MountedFilesystem`, rather than
through direct filesystem or platform API calls.

Keeping this boundary lets the engine stay deterministic, portable, and easy to
test with simulated or in-memory backends.
