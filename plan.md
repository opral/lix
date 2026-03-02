# Plan: Two-Layer Value Model With Canonical Boundaries (Breaking Change)

## Goal
Eliminate value-shape drift while preserving runtime ergonomics:
- Runtime APIs use raw JS values.
- Boundary/serialized APIs use one canonical envelope.

## Scope
- JS SDK, wasm-bindgen boundary, JS backends, Kysely adapter, Electron bridge, and CLI JSON output.
- `lix_file.data` must be represented consistently as binary.

## Runtime contract
- `LixRuntimeValue` is used in app-facing runtime SQL APIs:
  - `null`
  - `boolean`
  - `number`
  - `string`
  - `Uint8Array`
- `LixRuntimeQueryResult`: `{ columns: string[]; rows: LixRuntimeValue[][] }`.

## Canonical contract
- Define one canonical `LixCanonicalValue` envelope for boundary/wire/JSON surfaces:
  - `null`
  - `bool`
  - `int`
  - `float`
  - `text`
  - `blob` (base64 payload)
- `LixCanonicalQueryResult`: `{ columns: string[]; rows: LixCanonicalValue[][] }`.
- No mixed boundary representations.

## Steps

1. Freeze dual-contract schema spec
- Add ADR/RFC describing `LixRuntimeValue` and `LixCanonicalValue` contracts.
- Document strict invariants for `lix_file.data`:
  - runtime: `Uint8Array`
  - canonical: `blob(base64)`

2. Add cross-surface conformance tests first
- Create shared fixtures for all value kinds.
- Add assertions for runtime surfaces (raw values) and boundary surfaces (canonical values).

3. Rewrite wasm-bindgen value codec (source of truth)
- Keep wasm boundary strict and deterministic with explicit runtime<->canonical conversion points.
- Remove permissive/legacy input forms.

4. Make `@lix-js/sdk` strict and ergonomic
- Public execute/transaction APIs accept and return `LixRuntimeValue`.
- Move canonical encoding/decoding to explicit boundary helpers only.
- Remove implicit coercion paths.

5. Normalize JS backends to runtime output
- Ensure both `wasm-sqlite` and `better-sqlite3` return `LixRuntimeValue` cells.
- Ensure blob values are always emitted as `Uint8Array`.

6. Remove implicit unwrapping in `@lix-js/kysely`
- Driver path should receive raw runtime values directly.
- Keep only explicit logic needed for metadata extraction.

7. Unify transport layers
- Electron IPC: canonical values only.
- CLI:
  - `--format json` emits canonical values only (`LixCanonicalValue`).
  - table mode may keep human-readable formatting.

8. Update downstream consumers
- Migrate internal tools/apps to runtime values for local execution APIs.
- Use explicit canonical converters only at boundaries.
- Remove ad-hoc fallback logic for `0x...` and legacy envelopes.

9. Cleanup + release
- Remove legacy compatibility code and tests.
- Publish as a breaking major release with migration guide and examples.

## Non-goals
- Partial compatibility bridges that preserve multiple value representations long-term.
- Keeping implicit “smart” coercions in core execute APIs.

## Done criteria
- Runtime SQL APIs are raw-value ergonomic (`LixRuntimeValue`).
- Boundary/wire/JSON surfaces are canonical-only (`LixCanonicalValue`).
- `lix_file.data` is representation-stable across runtime and canonical layers.
- Conformance tests prevent reintroduction of drift.

## Progress log

- 2026-03-02 M1 complete: Canonical schema/spec frozen in [RFC 003](rfcs/003-canonical-lix-value/index.md) with strict `LixValue`/`LixQueryResult` invariants and explicit `lix_file.data` binary contract.
- 2026-03-02 M2 complete: Added conformance checks for canonical value shapes in `js-sdk` backend tests, `open-lix` blob roundtrip tests (`lix_file.data`), and CLI JSON unit tests.
- 2026-03-02 M3 complete: Rewrote wasm-bindgen boundary codec to canonical-only (`null|bool|int|float|text|blob(base64)`), including strict parse/validation and canonical `QueryResult` shape.
- 2026-03-02 M4 complete: Migrated `@lix-js/sdk` types from `LixValueLike`/`LixQueryResultLike` to canonical `LixValue`/`LixQueryResult`; removed object-to-JSON implicit coercion in SQL param normalization.
- 2026-03-02 M5 complete: Normalized JS SQLite backends to emit canonical row cells and canonical blob payloads, with stable `columns` output.
- 2026-03-02 M6 complete: Removed implicit value unwrapping in `@lix-js/kysely` query-row decode path; kept explicit integer extraction only for driver metadata (`changes()`, `last_insert_rowid()`).
- 2026-03-02 M7 partial: CLI `--format json` now emits canonical `LixValue` cells (including blob base64). Electron IPC layer is not present in this submodule and remains out-of-scope for this implementation pass.
- 2026-03-02 M8 partial: Updated immediate downstream tests/callers in `js-sdk`, `js-kysely`, and `js-better-sqlite3-backend` to canonical envelopes; broader ecosystem migration still required outside these packages.
- 2026-03-02 M9 pending: Release cleanup/migration guide not implemented in this code pass.
- 2026-03-02 Strategy update: Adopted two-layer naming and model: `LixRuntimeValue` for runtime APIs and `LixCanonicalValue` for boundary/serialized APIs.
- 2026-03-02 M4r complete: Implemented runtime-facing `LixRuntimeValue`/`LixRuntimeQueryResult` in JS SDK public types and APIs.
- 2026-03-02 M5r complete: Updated `wasm-sqlite` and `better-sqlite3` backends to accept/return raw runtime values (`number|string|null|Uint8Array`).
- 2026-03-02 M6r complete: `@lix-js/kysely` now operates on raw runtime rows directly (no canonical envelope handling in row decode).
- 2026-03-02 M7r complete: Kept canonical value envelopes at boundary surfaces; CLI JSON remains canonical (`kind` + `base64` for blobs).
- 2026-03-02 M8r complete: Added/updated runtime conformance tests for scalar and blob (`Uint8Array`) roundtrips in SDK/backends/Kysely packages.
- 2026-03-02 RFC update: RFC 003 now specifies the two-layer model (`LixRuntimeValue` + `LixCanonicalValue`).
- 2026-03-02 M8r follow-up: Re-enabled observe-recovery regression test using allowed tables (`lix_key_value`) and malformed JSON (`json_extract`) to validate stream recovery after transient query errors.
