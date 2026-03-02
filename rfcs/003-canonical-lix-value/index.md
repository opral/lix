# RFC 003: Two-Layer Value Model With Canonical Boundaries

## Status
Accepted

## Context
Value-shape drift across JS/wasm/backend/CLI boundaries introduced brittle decode logic and inconsistent handling of `lix_file.data`.

Observed drift patterns included:
- Mixed object wrappers and raw primitives.
- Binary values represented as `Uint8Array` in some APIs and `0x...` hex strings in CLI JSON.
- Adapter-level implicit unwrapping that masked inconsistencies.

## Decision
Adopt a two-layer value model:

1. Runtime contract for app-facing SQL APIs (`LixRuntimeValue`).
2. Canonical contract for boundary/wire/JSON surfaces (`LixCanonicalValue`).

Runtime contract:

```ts
export type LixRuntimeValue =
  | null
  | boolean
  | number
  | string
  | Uint8Array;

export type LixRuntimeQueryResult = {
  columns: string[];
  rows: LixRuntimeValue[][];
};
```

Canonical contract:

```ts
export type LixCanonicalValue =
  | { kind: "null"; value: null }
  | { kind: "bool"; value: boolean }
  | { kind: "int"; value: number }
  | { kind: "float"; value: number }
  | { kind: "text"; value: string }
  | { kind: "blob"; base64: string };

export type LixCanonicalQueryResult = {
  columns: string[];
  rows: LixCanonicalValue[][];
};
```

## Invariants
- Runtime SQL APIs accept/return `LixRuntimeValue`.
- Boundary/wire/JSON APIs accept/return `LixCanonicalValue`.
- `LixCanonicalQueryResult.columns` is always present and always a string array.
- `LixCanonicalQueryResult.rows` is always present and always a 2D array.
- `int.value` must be a finite integer and fit in signed 64-bit range.
- `float.value` must be a finite number.
- `blob.base64` uses RFC 4648 standard base64.
- `lix_file.data` is representation-stable:
  - runtime: `Uint8Array`
  - canonical: `{ kind: "blob", base64: string }`

## Consequences
- Runtime SQL APIs stay ergonomic and efficient (raw values, no base64 overhead in hot paths).
- Canonical boundary format remains deterministic for CLI/IPC/JSON transports.
- Legacy mixed forms are rejected at strict boundaries.
- Conversion is explicit at boundaries via dedicated runtime<->canonical codecs.
