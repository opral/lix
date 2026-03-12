# Execution Dispatch Plan

## Objective

Redesign the engine entry path around one rule:

`A statement that does not bind to a public surface must never enter the public SQL planning pipeline.`

The immediate trigger is the stack overflow in `engine::tests::unknown_read_query_returns_unknown_table_error`, but the real goal is architectural:

- cheap backend SQL should stay cheap
- public SQL should use the public planner only when actually needed
- classification should happen before heavy preparation

## Core Principle

The engine front door should be:

1. Parse once.
2. Classify early.
3. Dispatch to one narrow execution path.
4. Only pay for the machinery that path actually needs.

That means:

- plain backend reads/writes do not pay for public-surface lowering
- public reads/writes do not pay for unrelated generic preparation work
- transaction/script handling is explicit, not accidental fallout from the generic path

## Why This Plan Exists

Today, `Engine::execute_impl_sql()` routes even trivial non-public SQL through a broad preparation flow in `shared_path.rs`.

That broad path currently bundles together:

- runtime-function preparation
- file-write preprocessing
- requirement derivation
- read materialization checks
- public SQL preparation
- intent collection
- execution-plan building
- backend execution
- error normalization

For a query like:

```sql
SELECT * FROM unknown_table
```

that is the wrong architecture. The engine should be able to:

- parse it
- recognize it is not a public query
- send it straight to the backend
- normalize the backend error

instead of entering a stack-heavy planning path first.

## Non-Goals

This plan does not try to preserve the current execution seam.

It explicitly does not optimize for:

- backward compatibility with the existing monolithic preparation path
- keeping `shared_path.rs` as the one universal entrypoint
- preserving old `sql2`-era naming or layering assumptions

## Target Architecture

## 1. Early Classifier

Introduce a small, cheap classifier that runs immediately after parse.

Its job is only to answer:

- is this a public read?
- is this a public write?
- is this a plain backend statement batch?
- is this a transaction/script control path?
- does it require filesystem-side preprocessing?
- does it require read materialization?

It should avoid:

- canonicalization
- public lowering
- dependency derivation
- intent collection
- schema bootstrapping beyond what is necessary to identify public surfaces

## 2. Narrow Execution Paths

Replace the one broad preparation path with explicit routes:

### Path A: Plain Backend SQL

For statements that do not bind to public surfaces and do not need engine-specific rewriting.

Flow:

1. parse
2. classify as plain backend SQL
3. bind params
4. execute on backend
5. normalize backend error

This is the correct path for:

- `SELECT * FROM unknown_table`
- ordinary internal SQL when internal access is allowed
- non-public statements that do not need file-write or materialization machinery

### Path B: Public Read

For reads that bind to one or more public surfaces.

Flow:

1. parse
2. classify as public read
3. run public read lowering
4. optionally optimize
5. emit backend SQL
6. execute

### Path C: Public Write

For writes whose target is a public writable surface.

Flow:

1. parse
2. classify as public write
3. analyze write
4. resolve write plan
5. lower/partition execution
6. execute inside the engine transaction boundary

### Path D: Script / Transaction Control

For:

- explicit `BEGIN; ... COMMIT;` script handling
- public SQL session control
- multi-statement scripts that need statement barriers

This remains separate and explicit.

## 3. Lazy Runtime Preparation

Runtime-function setup and side-effect collection should be lazy.

Examples:

- deterministic mode lookup is not needed for a plain failing `SELECT * FROM unknown_table`
- filesystem insert-id rewriting is not needed for a simple backend read
- pending file write collection is not needed for read-only plain backend SQL

So each execution path should request only the pieces it actually uses.

## 4. Lightweight Error Normalization

Unknown-table and unknown-column normalization should not require the full public planner.

For plain backend SQL:

- use parsed relation names from the original statement
- use a lightweight table/column catalog source
- do not route back into public execution preparation

This rule exists to prevent “error formatting” from re-entering stack-heavy planning code.

## Design Rules

### Rule 1: Classification Owns Dispatch

The classifier decides which path owns correctness.

Downstream paths should not need to rediscover whether a statement was public or plain backend SQL.

### Rule 2: Plain Backend SQL Stays Off the Public Planner

Once a statement is classified as plain backend SQL, it must not:

- call public lowering
- build a public surface registry for planning
- run public dependency derivation
- enter public read/write preparation

### Rule 3: Heavy Preparation Is Opt-In

The engine should not prepare:

- runtime functions
- public lowering
- intent collection
- read materialization

unless the chosen path actually requires it.

### Rule 4: Error Paths Must Be Cheap

Error handling for simple backend failures must not be more expensive than the successful path would have been.

## Proposed Contracts

## Phase A: Parse Result

Input:

- raw SQL text
- raw params
- execution options

Output:

- parsed statements

## Phase B: Execution Classification

Define a small enum, for example:

```rust
enum ExecutionRoute {
    PlainBackendRead,
    PlainBackendWrite,
    PublicRead,
    PublicWrite,
    StatementScript,
    ExplicitTransactionScript,
}
```

And a small metadata struct, for example:

```rust
struct ExecutionDispatch {
    route: ExecutionRoute,
    requires_runtime_functions: bool,
    requires_file_preprocessing: bool,
    requires_read_materialization: bool,
}
```

The exact names can change, but the point is the same:

- classification output must be explicit
- route and prerequisites must be represented separately

## Phase C: Path-Specific Preparation

Each route gets its own preparation function.

Examples:

- `prepare_plain_backend_read(...)`
- `prepare_plain_backend_write(...)`
- `prepare_public_read(...)`
- `prepare_public_write(...)`
- `prepare_statement_script(...)`

No universal “prepare everything” function.

## Phase D: Path-Specific Execution

Execution should mirror preparation.

For example:

- `execute_plain_backend_statement_batch(...)`
- `execute_public_read(...)`
- `execute_public_write(...)`

This removes the need for broad branching after a single giant preparation step.

## Migration Strategy

## Phase 1: Introduce the Classifier

### Goal

Add an early classification layer without changing behavior yet.

### Tasks

1. Parse once in `Engine::execute_impl_sql()`.
2. Build `ExecutionDispatch`.
3. Log/assert the chosen route in tests.
4. Keep the existing broad path behind the dispatcher temporarily.

### Deliverable

The engine can classify statements before any heavy preparation begins.

## Phase 2: Split Plain Backend SQL Out of `shared_path`

### Goal

Move non-public statement execution off the public-preparation path entirely.

### Tasks

1. Add a direct plain-backend execution path.
2. Keep simple read-only/non-public writes off public preparation.
3. Run backend error normalization directly on the backend result.
4. Ensure `unknown_table` and similar failures do not enter public planning.

### Deliverable

Simple backend SQL bypasses the public planner entirely.

## Phase 3: Make Runtime Setup Lazy

### Goal

Only prepare runtime state when required by the selected route.

### Tasks

1. Delay deterministic-mode/runtime-function setup until a route asks for it.
2. Delay filesystem insert-id rewriting until a route asks for it.
3. Delay intent collection until a route asks for it.
4. Delay read materialization until a route asks for it.

### Deliverable

The plain backend route is cheap in both success and failure cases.

## Phase 4: Shrink `shared_path.rs`

### Goal

Turn `shared_path.rs` from the universal execution funnel into reusable helpers for the routes that still need it.

### Tasks

1. Move path-agnostic helpers into smaller modules.
2. Remove broad preparation responsibilities from `prepare_execution_with_backend()`.
3. Delete or rename the universal preparation function once the dispatcher owns path selection.

### Deliverable

There is no longer one monolithic preparation function for all SQL.

## Phase 5: Tighten Error Normalization

### Goal

Make unknown-table/unknown-column normalization lightweight and route-local.

### Tasks

1. Separate plain-backend error normalization from public-surface diagnostics.
2. Avoid public planner bootstrap in plain backend error paths.
3. Add regressions for unknown-table and unknown-column handling on the plain backend route.

### Deliverable

Error paths no longer re-enter heavy planning logic.

## Phase 6: Clean Cut

### Goal

Remove the old “prepare everything then branch” architecture.

### Tasks

1. Delete compatibility branches that preserve the monolithic flow.
2. Remove dead helpers that only existed for the universal path.
3. Rename the new seams clearly around dispatch/preparation/execution.

### Deliverable

The engine front door is dispatcher-first, not shared-path-first.

## Validation Sequence

For each slice, run at least:

- `cargo test -p lix_engine engine::tests::unknown_read_query_returns_unknown_table_error -- --exact`
- `cargo test -p lix_engine --lib --no-fail-fast`
- `cargo test -p lix_engine sqlite --no-fail-fast`

And keep fixing regressions in a loop until:

- plain backend errors stay off the public planner
- the engine lib tests pass on the default stack
- the SQLite-focused suite still passes

## Success Criteria

This redesign is successful when:

- `SELECT * FROM unknown_table` never enters public read preparation
- simple non-public SQL does not trigger deterministic/runtime-function setup unless needed
- public and non-public SQL have separate, explicit execution paths
- error normalization is route-local and cheap
- the engine no longer depends on large-stack wrappers for ordinary unit tests that only exercise classification/dispatch

## Progress Log

- 2026-03-12: Created `plan5.md` for the dispatcher-first execution redesign. The core rule is that non-public SQL must never enter the public planning pipeline, and the migration is organized around early classification, narrow execution paths, lazy preparation, and deletion of the monolithic `shared_path` funnel.
