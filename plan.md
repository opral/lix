# Canonical Wire Value Plan

## Goal
Stop exposing `lix_engine::Value` serde shape across SDK/FFI boundaries and enforce one canonical wire contract for all JS-facing value payloads.

## Canonical Contract
`LixCanonicalValue` must always be one of:

- `{ kind: "null", value: null }`
- `{ kind: "bool", value: boolean }`
- `{ kind: "int", value: number }`
- `{ kind: "float", value: number }`
- `{ kind: "text", value: string }`
- `{ kind: "blob", base64: string }`

No boundary may emit or accept legacy variant names (`Null`, `Bool`, `Integer`, `Real`, `Text`, `Blob`).

## Success Criteria

- Engine internals can keep `lix_engine::Value`, but wire serialization never depends on its derived serde enum shape.
- JS SDK boundary (`execute`, `beginTransaction.execute`, `observe`) is canonical-only.
- Legacy fallback in `packages/js-sdk/src/open-lix.ts` is removed.
- Tests fail if legacy wire kinds appear again.

## Implementation Plan

1. Introduce explicit wire value types in engine.
2. Add deterministic conversion functions between internal and wire values.
3. Route all JS/FFI serialization through these conversions.
4. Add conformance tests for execute, transaction execute, and observe.
5. Remove JS fallback and keep strict decoding.

## Phase 1: Engine Wire Types

- Add module in engine for wire contracts, for example:
  - `packages/engine/src/wire/value.rs`
  - `packages/engine/src/wire/mod.rs`
- Define canonical wire enums/structs with explicit serde names.
- Avoid derive defaults that leak Rust variant names.
- Add conversion APIs:
  - `impl From<&Value> for WireValue` (or explicit `to_wire_value(&Value)`).
  - `impl TryFrom<&WireValue> for Value` (or explicit `from_wire_value(&WireValue)`).
- Add `WireQueryResult` for `rows` and `columns`.

## Phase 2: Boundary Refactor

- Replace ad-hoc/manual value JSON encoding in:
  - `packages/js-sdk/wasm-bindgen.rs`
- Use engine wire conversion helpers for:
  - `query_result_to_js`
  - `value_from_js`
  - any query-result/observe payload serialization path
- Ensure observe dedupe key serialization does not depend on internal enum variant names:
  - `packages/engine/src/observe.rs` (`observe_source_key`)
  - serialize params via canonical wire shape before hashing/stringifying.

## Phase 3: Strict JS SDK Decoder

- Remove legacy fallback branches from:
  - `packages/js-sdk/src/open-lix.ts` (`decodeCanonicalValue`)
- Keep strict canonical validation with actionable error messages.

## Test Plan (Regression Coverage)

### Engine tests (Rust)

- Add unit tests for `Value <-> WireValue` conversion in new wire module:
  - null/bool/int/float/text/blob roundtrip
  - blob base64 encode/decode correctness
  - int range/finite validation
- Add serialization snapshot/assertion test:
  - canonical JSON includes only lowercase kinds
  - no `Null/Bool/Integer/Real/Text/Blob`
- Add observe key serialization test:
  - two equivalent param sets produce stable canonical key
  - key payload uses canonical kinds only

### Wasm boundary tests (Rust/JS integration)

- Add tests for wasm-bindgen boundary conversion:
  - execute returns canonical wire values for all primitive kinds
  - beginTransaction.execute returns canonical wire values
  - observe event rows return canonical wire values
- Add negative tests for invalid incoming values:
  - reject non-canonical kind names
  - reject wrong field shape (`blob` missing `base64`, etc.)

### JS SDK tests (Vitest)

- Add canonical conformance tests in `packages/js-sdk/src/open-lix.test.ts` (or a dedicated `open-lix.canonical-wire.test.ts`):
  - `execute` rows contain only canonical kinds at wasm boundary
  - `beginTransaction.execute` rows contain only canonical kinds
  - `observe.next()` rows contain only canonical kinds
- Add strict-decoder tests:
  - legacy payload shape throws `TypeError`
  - non-object/invalid kind payload throws `TypeError`

### Guardrail test

- Add a cross-boundary conformance test that fails on any of:
  - `kind: "Null" | "Bool" | "Integer" | "Real" | "Text" | "Blob"`

## Migration and Rollout

1. Land wire types + conversion helpers first.
2. Switch wasm-bindgen paths to wire helpers.
3. Land tests while fallback is still present.
4. Remove JS fallback.
5. Run full `packages/js-sdk` and engine tests.

## Out of Scope

- Backward compatibility with older non-canonical wasm artifacts.
- Any new SQL behavior changes unrelated to wire value encoding.

## Notes

- Current observe failures (`stateCommitSequence` null and malformed JSON in key-value query path) are separate issues and should be debugged independently after wire-contract hardening.
