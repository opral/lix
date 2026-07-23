# Async component `stream<u8>` transfer probe

This isolated experiment compares the Component Model async ABI's `stream<u8>`
against the current whole-buffer `list<u8>` ABI. It performs no storage or CSV
work. Two guest workloads isolate transfer from parser-like scanning:

- `count`: drain the payload and return its length.
- `checksum`: drain and scan every byte.

The stream guest reuses a bounded buffer. The matrix tests 8 KiB, 64 KiB, and
1 MiB chunks. `stream-u8-vec-64k` gives list and stream the same owned host
input, while the `stream-u8-bytes-*` cases exercise Wasmtime's direct `Bytes`
producer support. Host input preparation is outside the timer.

Build and run:

```sh
# From <repo-root>:
cargo build --manifest-path experiments/p3-stream-probe/Cargo.toml \
  -p p3-stream-probe-guest --target wasm32-wasip2 --release
cargo run --manifest-path experiments/p3-stream-probe/Cargo.toml \
  -p p3-stream-probe-host --release -- \
  target/wasm32-wasip2/release/p3_stream_probe_guest.wasm
```

The Rust toolchains installed for this repository recognize the Tier-3
`wasm32-wasip3` target but do not distribute its standard library. The guest is
therefore built as a WASI P2 reactor while its exported benchmark interface uses
the same async Canonical ABI and `stream<u8>` machinery used by WASI P3. The
host enables Wasmtime 47.0.2's experimental P3 linker and async component
model. This measures
the proposed Lix API boundary without pretending the installed Rust target is a
production-ready P3 SDK. See [RESULTS.md](RESULTS.md) for the retained matrix,
toolchain blocker, and architectural interpretation.
