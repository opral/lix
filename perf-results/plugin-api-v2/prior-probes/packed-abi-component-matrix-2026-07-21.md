# Wasmtime Component ABI: rich WIT values vs packed arena

Date: 2026-07-21

This is an ABI-isolation benchmark. Both implementations are real
`wasm32-wasip2` components called through Wasmtime's Component Model. Guest
functions perform only header/length validation and move owned fields; there is
no CSV parsing, diffing, storage, or SQLite work in the timed region.

## Host and build

- Host: MacBook Pro `Mac17,9`, Apple M5 Pro, 18 cores, 64 GB RAM
- OS: macOS 26.3.1 (25D71), Darwin 25.3.0 arm64
- Lix commit: `115350f90a7610a56ba49a07f725b501f61e8f31`
- Rust: `rustc 1.97.0-nightly (b954122bb 2026-05-20)`
- Cargo: `cargo 1.97.0-nightly (4d1f98451 2026-05-15)`
- Wasmtime: 45
- Rich guest component: 72,210 bytes
- Packed guest component: 66,094 bytes

Build and run commands:

```sh
# From <repo-root>:
cargo build --release -p lix_plugin_abi_bench
target/release/lix_plugin_abi_bench
```

Configuration: 2 warmups, adaptive 9-31 measured samples per case, 1.5 s
sampling target, 512 MiB aggregate guest-linear-memory ceiling for comparable
timings, plus a fresh call under the production 64 MiB (67,108,864-byte)
ceiling. All values below are microseconds except memory, which is bytes.

## Operations

- `detect-empty`: exact current rich `list<entity-state> + file` input shape and
  an empty `list<detected-change>` output. Packed passes one versioned arena and
  returns an empty arena. This isolates input lowering/lifting and allocation.
- `entity-rt`: file is empty; rich moves every entity into a detected-change
  result, while packed changes the packet-kind word and returns its state arena.
  This includes rich output lifting without guest-side field clones.
- `file-rt`: state is empty; rich returns the input file bytes and packed returns
  its 32-byte header plus file bytes. This is the Component Model byte-copy
  control.

Arena construction and rich host-value construction are deliberately outside
the timed region. A production packed design must keep data encoded or build
the arena directly from storage; rebuilding it from the current rich object
graph on every call is not covered by these numbers.

## Full matrix

`R n` and `P n` are rich and packed sample counts. `R peak` and `P peak` are
aggregate guest linear-memory high-water marks. Every fresh 64 MiB probe passed.

| Logical size | Snapshot/entity | Entities | Operation | R n | Rich p50 | Rich p95 | P n | Packed p50 | Packed p95 | p50 speedup | R peak | P peak | 64 MiB |
|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 102,400 | 48 | 2,134 | detect-empty | 31 | 387.667 | 503.375 | 31 | 5.791 | 9.833 | 66.94x | 1,703,936 | 1,441,792 | pass |
| 102,400 | 48 | 2,134 | entity-rt | 31 | 687.417 | 1,132.125 | 31 | 8.459 | 12.709 | 81.27x | 1,703,936 | 1,376,256 | pass |
| 102,400 | 48 | 2,134 | file-rt | 31 | 4.792 | 11.625 | 31 | 4.667 | 6.042 | 1.03x | 1,179,648 | 1,179,648 | pass |
| 102,400 | 1,024 | 100 | detect-empty | 31 | 21.292 | 22.834 | 31 | 2.750 | 11.958 | 7.74x | 1,310,720 | 1,310,720 | pass |
| 102,400 | 1,024 | 100 | entity-rt | 31 | 37.708 | 51.584 | 31 | 4.959 | 26.417 | 7.60x | 1,179,648 | 1,179,648 | pass |
| 102,400 | 1,024 | 100 | file-rt | 31 | 4.833 | 5.250 | 31 | 4.416 | 4.875 | 1.09x | 1,179,648 | 1,179,648 | pass |
| 1,048,576 | 48 | 21,846 | detect-empty | 31 | 4,315.875 | 4,842.792 | 31 | 57.291 | 102.375 | 75.33x | 7,471,104 | 4,915,200 | pass |
| 1,048,576 | 48 | 21,846 | entity-rt | 31 | 7,844.167 | 12,163.167 | 31 | 91.083 | 96.583 | 86.12x | 7,274,496 | 3,866,624 | pass |
| 1,048,576 | 48 | 21,846 | file-rt | 31 | 36.125 | 44.334 | 31 | 35.584 | 60.292 | 1.02x | 2,162,688 | 2,162,688 | pass |
| 1,048,576 | 1,024 | 1,024 | detect-empty | 31 | 222.125 | 279.791 | 31 | 32.250 | 35.291 | 6.89x | 3,407,872 | 3,276,800 | pass |
| 1,048,576 | 1,024 | 1,024 | entity-rt | 31 | 407.666 | 517.917 | 31 | 37.541 | 38.792 | 10.86x | 2,359,296 | 2,228,224 | pass |
| 1,048,576 | 1,024 | 1,024 | file-rt | 31 | 34.959 | 38.833 | 31 | 33.875 | 54.875 | 1.03x | 2,162,688 | 2,162,688 | pass |
| 5,242,880 | 48 | 109,227 | detect-empty | 31 | 21,299.625 | 33,165.209 | 31 | 478.250 | 657.167 | 44.54x | 32,964,608 | 20,185,088 | pass |
| 5,242,880 | 48 | 109,227 | entity-rt | 30 | 36,303.250 | 44,570.292 | 31 | 476.209 | 644.000 | 76.23x | 32,112,640 | 14,942,208 | pass |
| 5,242,880 | 48 | 109,227 | file-rt | 31 | 168.167 | 179.041 | 31 | 171.875 | 245.833 | 0.98x | 6,356,992 | 6,356,992 | pass |
| 5,242,880 | 1,024 | 5,120 | detect-empty | 31 | 1,114.625 | 1,253.083 | 31 | 167.167 | 204.792 | 6.67x | 12,582,912 | 11,993,088 | pass |
| 5,242,880 | 1,024 | 5,120 | entity-rt | 31 | 2,039.667 | 2,308.709 | 31 | 182.041 | 266.084 | 11.20x | 7,536,640 | 6,750,208 | pass |
| 5,242,880 | 1,024 | 5,120 | file-rt | 31 | 171.084 | 185.875 | 31 | 170.334 | 234.041 | 1.00x | 6,356,992 | 6,356,992 | pass |
| 10,485,760 | 48 | 218,454 | detect-empty | 25 | 46,089.833 | 54,482.084 | 31 | 675.667 | 1,004.500 | 68.22x | 64,880,640 | 39,321,600 | pass |
| 10,485,760 | 48 | 218,454 | entity-rt | 15 | 81,283.500 | 99,488.333 | 31 | 968.042 | 1,267.000 | 83.97x | 63,111,168 | 28,835,840 | pass |
| 10,485,760 | 48 | 218,454 | file-rt | 31 | 341.417 | 359.042 | 31 | 359.709 | 504.833 | 0.95x | 11,599,872 | 11,599,872 | pass |
| 10,485,760 | 1,024 | 10,240 | detect-empty | 31 | 2,402.000 | 3,222.584 | 31 | 384.084 | 635.916 | 6.25x | 24,051,712 | 22,872,064 | pass |
| 10,485,760 | 1,024 | 10,240 | entity-rt | 31 | 6,165.667 | 9,693.792 | 31 | 380.792 | 457.250 | 16.19x | 13,959,168 | 12,386,304 | pass |
| 10,485,760 | 1,024 | 10,240 | file-rt | 31 | 356.291 | 423.625 | 31 | 346.625 | 401.959 | 1.03x | 11,599,872 | 11,599,872 | pass |

## Encoded input sizes

The rich column counts string/file content bytes, not canonical-ABI headers or
allocator metadata. Packed sizes are the exact arena lengths, including the
header, record directory, primary-key span table, and file bytes. The sparse
packed payload is therefore larger, making its speedup conservative with
respect to bytes copied.

| Logical size | Snapshot/entity | Rich state content | Rich detect input | Packed state arena | Packed full arena | Packed file arena |
|---:|---:|---:|---:|---:|---:|---:|
| 102,400 | 48 | 185,626 | 288,039 | 271,018 | 373,418 | 102,432 |
| 102,400 | 1,024 | 106,300 | 208,713 | 110,332 | 212,732 | 102,432 |
| 1,048,576 | 48 | 1,900,570 | 2,949,159 | 2,774,442 | 3,823,018 | 1,048,608 |
| 1,048,576 | 1,024 | 1,088,512 | 2,137,101 | 1,129,504 | 2,178,080 | 1,048,608 |
| 5,242,880 | 48 | 9,502,741 | 14,745,634 | 13,871,853 | 19,114,733 | 5,242,912 |
| 5,242,880 | 1,024 | 5,442,560 | 10,685,453 | 5,647,392 | 10,890,272 | 5,242,912 |
| 10,485,760 | 48 | 19,005,466 | 29,491,239 | 27,743,658 | 38,229,418 | 10,485,792 |
| 10,485,760 | 1,024 | 10,885,120 | 21,370,893 | 11,294,752 | 21,780,512 | 10,485,792 |

## Native copy controls

The native control is one `copy_from_slice` into a preallocated, hot
destination. The packed-full controls use the exact full-arena lengths.

| Bytes copied | Case | p50 | p95 |
|---:|---|---:|---:|
| 102,400 | file only | 0.834 | 0.959 |
| 373,418 | 48-byte packed full | 4.417 | 4.500 |
| 212,732 | 1,024-byte packed full | 2.542 | 13.125 |
| 1,048,576 | file only | 13.959 | 32.042 |
| 3,823,018 | 48-byte packed full | 59.292 | 124.125 |
| 2,178,080 | 1,024-byte packed full | 32.500 | 47.333 |
| 5,242,880 | file only | 86.833 | 179.667 |
| 19,114,733 | 48-byte packed full | 369.958 | 730.542 |
| 10,890,272 | 1,024-byte packed full | 170.000 | 245.500 |
| 10,485,760 | file only | 166.083 | 183.833 |
| 38,229,418 | 48-byte packed full | 667.791 | 1,069.958 |
| 21,780,512 | 1,024-byte packed full | 401.000 | 581.166 |

## Interpretation

1. Per-entity canonical lowering/lifting and allocation dominate. Packed input
   is 6.25-75.33x faster and packed rich-output replacement is 7.60-86.12x
   faster across the tested matrix. This clears the isolated 20% mechanism
   screen by orders of magnitude, but it is not an adoption result; a packed
   production path must still clear the full-engine RocksDB/SlateDB >20% gate.
2. The gain scales with entity count, not just bytes. At 10 MiB, moving from
   10,240 1-KiB entities to 218,454 48-byte entities changes rich detect from
   2.402 ms to 46.090 ms, while packed changes from 0.384 ms to 0.676 ms.
3. Raw `list<u8>` round-trip is already near the copy floor and the two ABIs
   are effectively tied. This falsifies a generic Wasm-engine explanation for
   the entity result and points directly at nested canonical values and guest
   allocation.
4. The current rich ABI has almost no 64 MiB headroom for a 10 MiB sparse file:
   detect peaks at 64,880,640 bytes, only 2,228,224 bytes below the ceiling.
   Packed peaks at 39,321,600 bytes, 39.4% lower. Rich entity round-trip peaks
   at 63,111,168 bytes versus packed's 28,835,840 bytes, 54.3% lower.
5. This prototype supports a hard-breaking, versioned packed plugin ABI while
   keeping plugins in Wasm. It does not by itself select a serialization format
   or prove the end-to-end cost once plugin parsing/diffing and storage-to-arena
   encoding are included.
