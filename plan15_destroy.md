# Plan 15: Backend-Owned Storage Destroy And WAL Lifecycle

## Goal

Make backend ownership of physical storage explicit so callers never manage backend-specific persistence artifacts themselves.

This plan is intentionally narrow:

- always use WAL for SQLite
- keep the backend API simple
- do not introduce a workload hint API yet
- do not introduce a separate `LixStorage` abstraction

## Problem

Today callers still know too much about physical storage.

Examples:

- `git-replay --force` deletes only the main `.lix` file
- tests already know they must also delete `-wal`, `-shm`, and `-journal`
- WAL lifecycle is not established as a backend invariant on open

That violates the intended abstraction boundary.

The result is predictable:

- callers accidentally leave backend-owned artifacts behind
- retry behavior becomes nondeterministic
- SQLite-specific failure modes leak into application code

## First Principle

`LixBackend` represents a concrete persistence target, not only a SQL executor.

Callers may ask the backend to:

- execute SQL
- open transactions
- destroy the target

Callers may not:

- delete backend-owned files directly
- issue backend-maintenance commands directly
- know which physical artifacts exist for a given backend

For SQLite, the main database file, WAL file, and SHM file are one storage unit owned by the backend.

For Postgres, the schema/database objects are the storage unit owned by the backend.

For wasm/opfs SQLite, the OPFS-backed database state is the storage unit owned by the backend.

## Backend Contract

Extend `LixBackend` with one lifecycle operation:

- `destroy()`

Keep the rest of the contract unchanged for now:

- `dialect()`
- `execute(...)`
- `begin_transaction(...)`
- existing image export/restore methods

Do not add:

- checkpoint APIs
- workload hint APIs
- storage-side session management APIs

Those may come later if needed, but they are not required to fix the current abstraction leak.

## Required Invariants

### 1. Backend-owned destroy

`destroy()` must remove or clear the full persistence target for that backend.

This means:

- SQLite native: main db + WAL + SHM, and any equivalent journal artifacts the implementation owns
- SQLite wasm/opfs: the persisted OPFS target
- Postgres: the configured schema/database target according to backend scope

### 2. Caller ignorance

No caller may hardcode backend-specific artifact deletion.

Delete:

- CLI code that removes `.lix` files directly as a reset primitive
- test helpers that encode storage cleanup policy outside the backend

### 3. WAL as backend invariant

For SQLite, WAL mode must be established and maintained inside the backend implementation.

The caller must not:

- choose WAL vs non-WAL
- run `wal_checkpoint`
- reason about `-wal` / `-shm`

This plan does not yet define the exact checkpoint policy, only the ownership boundary.

### 4. Destroy is physical, not logical

`destroy()` is a physical storage lifecycle operation.

It is not:

- a SQL command
- a logical Lix reset
- a live-state rebuild

Higher-level flows remain:

1. destroy target
2. initialize target
3. open target

## Required Structural Changes

### 1. Add `destroy()` to `LixBackend`

Update the backend trait so every concrete backend must define its physical destroy semantics.

This should be treated as part of the core backend contract, not as an optional helper.

### 2. Implement backend-specific destroy semantics

#### SQLite native

Implement `destroy()` against the backend’s configured path.

It must own deletion of:

- main database file
- WAL file
- SHM file
- any journal file variant still relevant to the implementation

The implementation should tolerate absent artifacts and should return a backend error only for real destroy failures.

#### Other backends

Each backend must implement its own physical destroy behavior in backend-native terms.

The public contract stays semantic:

- destroy the target represented by this backend instance

### 3. Route `--force` through backend destroy

Replace direct file deletion in:

- `packages/cli/src/commands/exp/git_replay.rs`

with backend-owned destroy semantics.

The CLI should no longer know how many files or storage objects belong to a database target.

### 4. Centralize test cleanup through backend policy

Replace ad hoc cleanup helpers that delete:

- main file
- `-wal`
- `-shm`
- `-journal`

with backend-owned destroy or a thin backend-specific helper that is implemented next to the backend, not inside unrelated tests.

## Non-Goals

This plan does not yet include:

- a generalized maintenance API
- checkpoint progress hooks
- replay-specific workload hints
- history cache materialization changes
- path encoding behavior changes

Those are separate concerns.

## Success Criteria

- No CLI command deletes backend artifacts directly as a reset primitive.
- `git-replay --force` uses backend-owned destroy behavior.
- The backend trait clearly owns physical storage lifecycle.
- SQLite WAL/SHM lifecycle details are no longer visible in caller code.
- New backends can define destroy semantics without changing caller logic.

## Expected Outcome

After this change, the abstraction boundary becomes coherent:

- callers operate on a backend
- backends own persistence targets
- WAL artifacts are implementation details

That does not fully solve every WAL reliability issue by itself, but it establishes the correct boundary so WAL fixes land in one place instead of leaking into CLI and tests.

## Progress Log

- Checkpoint 1: extended `LixBackend` with backend-owned `destroy()` semantics and kept the API narrow without adding hint/session APIs.
- Checkpoint 2: routed `git-replay` output cleanup through backend-owned SQLite destroy behavior instead of direct file deletion.
- Checkpoint 3: centralized native SQLite artifact cleanup in the rs-sdk backend and switched rs-sdk tests to use backend-owned destroy helpers.
- Checkpoint 4: verified the change with targeted tests:
  - `cargo test -p lix_rs_sdk --test execute --test transaction`
  - `cargo test -p lix_cli prepare_output_lix_path_force_removes_existing_file_and_sidecars -- --nocapture`
- Checkpoint 5: cleaned up the CLI boundary so `.lix` output preparation now routes through `packages/cli/src/db/mod.rs` instead of embedding storage-target policy inside `git_replay.rs`.

## Progress log
