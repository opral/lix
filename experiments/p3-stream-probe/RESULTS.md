# Results

Measured July 21, 2026 on an 18-core Apple M5 Pro with 64 GiB RAM. The host
was Wasmtime 45.0.3 and the guest was an 84.7 KiB Wasm component. Every cell is
40 warm samples after eight warmups. Host input preparation and expected-result
calculation were outside the timer.

The `list<u8>` guest export itself uses the synchronous Canonical ABI, matching
the current Lix shape. Because the same Wasmtime store has Component Model async
enabled for the stream export, the embedder must invoke that synchronous export
through Wasmtime's `call_async` fiber API. This small host-side cost biases the
comparison slightly in favor of the stream; it does not weaken the conclusion
that streaming produced no meaningful latency win.

`guest high-water` is the largest single linear-memory size requested through
Wasmtime's `ResourceLimiter`. It is not aggregate component memory or process
RSS. This probe's main guest memory is the growing memory; its baseline is about
1.06 MiB, so the difference above that baseline is the useful payload comparison.

## 10 MiB input

| Boundary | Count p50 / p95 | Checksum p50 / p95 | Guest high-water |
|---|---:|---:|---:|
| `list<u8>` | 0.150 / 0.164 ms | 2.692 / 2.765 ms | 11.062 MiB |
| `stream<u8>`, `Vec`, 64 KiB | 0.201 / 0.389 ms | 2.717 / 2.802 ms | 1.125 MiB |
| `stream<u8>`, `Bytes`, 8 KiB | 0.486 / 0.860 ms | 2.813 / 2.894 ms | 1.062 MiB |
| `stream<u8>`, `Bytes`, 64 KiB | 0.175 / 0.236 ms | 2.718 / 2.804 ms | 1.125 MiB |
| `stream<u8>`, `Bytes`, 1 MiB | 0.142 / 0.163 ms | 2.686 / 2.820 ms | 2.062 MiB |

For the parser-like checksum, the tuned 1 MiB stream was effectively tied with
the list: -0.2% at p50 and +2.0% at p95. It reduced guest linear-memory
high-water by 81%. A 64 KiB stream reduced high-water by 90% and cost about 1%
at checksum p50. Streaming did not produce a latency improvement over 10%.

For drain-only transfer, small chunks expose scheduling overhead: 8 KiB was
over 3x the list p50, 64 KiB was 17% slower with the `Bytes` producer, and 1 MiB
was 5% faster. Absolute times are below 0.5 ms, so the checksum results are the
more relevant proxy for parsing.

## 1 MiB input

| Boundary | Count p50 / p95 | Checksum p50 / p95 | Guest high-water |
|---|---:|---:|---:|
| `list<u8>` | 0.016 / 0.018 ms | 0.271 / 0.474 ms | 2.062 MiB |
| `stream<u8>`, `Vec`, 64 KiB | 0.019 / 0.020 ms | 0.276 / 0.293 ms | 1.125 MiB |
| `stream<u8>`, `Bytes`, 8 KiB | 0.031 / 0.034 ms | 0.284 / 0.289 ms | 1.062 MiB |
| `stream<u8>`, `Bytes`, 64 KiB | 0.019 / 0.049 ms | 0.276 / 0.281 ms | 1.125 MiB |
| `stream<u8>`, `Bytes`, 1 MiB | 0.013 / 0.014 ms | 0.266 / 0.272 ms | 2.062 MiB |

The 1 MiB list checksum p95 has OS-noise outliers in the retained run. No claim
uses an unretained repeat; the interpretation below relies on the retained p50
and 10 MiB capacity/memory matrix.

## Interpretation for Lix

- Do not adopt P3 streams as a latency optimization by themselves. On a
  CPU-bound byte scan they were neutral within measurement noise.
- Adopt a streaming boundary if large-file capacity and bounded memory matter.
  At 10 MiB it removed 9-10 MiB of guest payload high-water. That directly
  addresses the current 64 MiB guest ceiling, but only if parsing is incremental.
- Start chunk tuning around 256 KiB-1 MiB. An 8 KiB default is visibly too small
  for in-process CPU parsing. A 64 KiB chunk is a reasonable memory-first point,
  but 1 MiB removed almost all transfer overhead while remaining bounded.
- Prefer Wasmtime's `Bytes` producer when the host already owns reference-counted
  bytes. The benchmark deliberately excluded input cloning, so this result does
  not count the additional real-world benefit of avoiding a caller-side `Vec`
  clone.
- Input streaming alone does not fix the current plugin API's full entity-state
  input or full change-list/render output. Meaningful end-to-end gains require an
  incremental parser plus incremental/paged entity exchange or a persistent
  guest document and patch-oriented output.

## Toolchain boundary

The component binary proves this is the async Canonical ABI, not a simulated
channel: `wasm-tools print` shows `canon stream.read ... async`, `canon
task.return`, and async-lifted exports.

The pinned Rust nightly recognizes the Tier-3 `wasm32-wasip3` target, but it
does not ship a prebuilt standard library for it. Building std locally reached
the linker and failed with `unable to find library -lc`. Rust's target
documentation requires an external WASI SDK sysroot; none is installed here.
The measured guest is consequently a WASI P2 reactor exporting a genuine P3
`async func`/`stream<u8>` API, hosted with Wasmtime 45's P3 linker. This isolates
the proposed Lix boundary fairly without claiming the incomplete Rust P3 SDK is
production-ready.
