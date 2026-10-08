# SQL content checksum profile

Measured on 2026-10-08 with the change based on `3f25bf40b`.

## Environment and method

- Apple M5 Pro, 64 GiB RAM, macOS 26.3.1, aarch64.
- Rust `1.97.0-nightly (b954122bb 2026-05-20)`, optimized release build.
- RocksDB plus the canonical in-memory storage control, each in a fresh process.
- Deterministic pseudorandom bytes with a different seed for each size, avoiding
  highly compressible input and shared prefixes between files.
- Twenty samples per query after warming both query plans and file content.
  Read/hash order alternates. Median averages samples 10 and 11; p95 is sample 19.
- Fixture construction, ingestion, and reference SHA-256 computation are outside
  query timings. Each checksum is checked against the Rust `sha2` reference.
- Final runs started after the builds and test suite finished. These are local,
  warm-cache SQL measurements, excluding HTTP transport and cold disk reads.

Queries:

```sql
SELECT content FROM lix_file WHERE path = $1;
SELECT encode(sha256(content), 'hex') AS content_sha256
FROM lix_file WHERE path = $1;
```

## Results

All times are milliseconds. The read and checksum paths differ in query planning
and result materialization, so their difference is not an isolated SHA-256 CPU
measurement.

| Backend | File size | Read median | Read p95 | SHA-256 median | SHA-256 p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| rocksdb | 45 KiB | 0.111 | 0.273 | 0.329 | 0.798 |
| rocksdb | 1 MiB | 0.782 | 1.236 | 1.612 | 2.266 |
| rocksdb | 16 MiB | 10.561 | 11.752 | 16.352 | 20.963 |
| rocksdb | 63 MiB | 40.489 | 52.061 | 64.339 | 70.313 |
| memory | 45 KiB | 0.069 | 0.163 | 0.281 | 0.670 |
| memory | 1 MiB | 0.678 | 0.971 | 1.595 | 1.953 |
| memory | 16 MiB | 8.396 | 9.806 | 13.941 | 15.588 |
| memory | 63 MiB | 30.612 | 32.516 | 50.271 | 53.308 |

The checksum result is always 64 text bytes, versus the full file bytes for the
read (excluding result envelopes and row bookkeeping). For the 45 KiB SVG-sized
case, RocksDB verification took 0.329 ms median. At 63 MiB, it took 64.339 ms;
the in-memory control took 50.271 ms. Full-file processing grows with input size.
This provides inexpensive verification for small files and avoids transferring
large contents to the caller, while retaining the cost of loading and hashing
all input bytes on the engine.

A preliminary 64 MiB full-content read hit `LIX_READ_RESOURCE_EXHAUSTED`: the fixed
64 MiB buffered-result budget also charges row/value overhead. The final largest
fixture is 63 MiB so both queries can be compared below that budget.

Whole-process peak RSS from macOS `/usr/bin/time -l` was 634.0 MiB for RocksDB and
304.2 MiB for memory. These include fixture ingestion, retained repository data,
all four sizes, and storage caches; they are not isolated hash allocation costs.
The SQL function processes materialized file content, rather than streaming it.

Raw measurements are in [content-checksum-profile.jsonl](./content-checksum-profile.jsonl).

## Reproduce

```sh
cargo run --manifest-path tooling/Cargo.toml -p lix_e2e --release \
  --example content_checksum_profile
LIX_PROFILE_MEMORY=1 cargo run --manifest-path tooling/Cargo.toml -p lix_e2e \
  --release --example content_checksum_profile
```

## Validation

- Full engine simulations: 4,837 passed, 74 skipped, including the new checksum
  regression in both base and tracked-state-rebuild simulations.
- `cargo test -p lix --doc`: 10 passed.
- Regression cases: empty bytes, `abc`, UTF-8, non-UTF-8 binary bytes, a file larger
  than the maximum CAS chunk, replacement writes, scalar hashing, NULL propagation,
  SHA-256/digest equivalence, and base64 encoding/decoding round trips.
- Both release profiles verified every digest against the reference before timing.
- Targeted Rust formatting and `git diff --check` passed.

The full simulation command used `RUST_MIN_STACK=16777216` as required by the
engine's AGENTS.md. Apple's default linker failed on the large test binary with
an ARM64 branch-range error. Bundled LLVM LLD 22.1.4 linked it, but its unstripped
4.2 GiB output exceeded Mach-O code-signature offset limits. The successful local
run used a temporary `cc` wrapper invoking Xcode clang with LLD, the SDK sysroot,
`-Wl,-exported_symbol,_main -Wl,-x -Wl,-S`; this reduced the executable to 1.4 GiB.
The wrapper was supplied through PATH only for the test command; repository build
configuration and release profiling binaries were unchanged.
