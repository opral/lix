# Verified-edit CAS delta results

The follow-up connects the format-neutral WASM edit stream to the binary CAS's
bounded copy/insert representation. The interface contains no file-format,
content-type, Git, or LFS policy. The format is a hard cut; existing databases
are not migrated.

Git packfiles establish the core representation: a deltified object names a
base and reconstructs its result with copy-from-base and inline-insert
instructions. Lix uses the same primitives but deliberately flattens every
edit onto one full base, avoiding Git's delta-chain read amplification. The
manifest embeds the base's physical layout, so a cold delta read needs no
dependent base-manifest lookup. See Git's [pack format][git-pack].

The surrounding layout follows RocksDB's key/value-separation guidance:
compact manifests live apart from immutable content chunks, and related
metadata is kept away from bulky values. RocksDB documents both the reduction
in compaction write amplification from immutable blob files and the benefit of
separating file metadata from file-block data in the key space. See
[BlobDB][rocks-blobdb] and [key layout][rocks-key-layout].

## Policy

- Any WASM component plugin with blob materialization can participate. Its
  transition must produce one host-verified coalesced edit against the exact
  accepted document. Full output bytes and their BLAKE3 hash remain
  authoritative.
- There is no plugin manifest flag and no text/binary branch in the CAS API.
  Derived materializations do not use the binary CAS; raw or initial writes
  without a verified base edit use the normal full-blob path.
- Files smaller than 512 bytes use the normal full-blob path.
- A delta has at most 32 segments and at most the smaller of 64 KiB or 12.5%
  of the resulting file in inserted bytes.
- Editing a delta composes its piece table against the same canonical full
  base. A delta never references another delta. Crossing a bound writes a new
  full blob, which becomes the next base.
- Every inserted content chunk and every reconstructed delta is verified
  against its content-addressed key.

Fine-grained FastCDC policies at 8 KiB and 64 KiB were rejected. The 8 KiB
policy made the microbenchmark 30.5% faster but regressed Wesnoth/SlateDB
replay by 3.4%; the 64 KiB policy made the microbenchmark 23.7% faster but
regressed aggregate replay by 1.9%. The retained verified-edit policy is
neutral in aggregate replay time and lets any plugin benefit when it can prove
the same format-independent edit contract.

## Git replay corpus

The baseline is `main` at `b51aacaf7` with the RocksDB Zstd and final
point-write changes. The candidate release binary SHA-256 is
`6c0b5b5f8ce420c98adf22c731dda57c74fcaa71f850afdbfa5e82981257e095`.
Every run used `lix exp git-replay --plugins all`, scoped parent bootstrap,
explicit checkpoints, storage flush, and final Git-tree verification. Database
bytes are `du -sb` after close.

| repository | adapter | replay before | replay after | time delta | bytes before | bytes after | size delta |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `microsoft/vscode-docs`, 100 commits | RocksDB | 4624.477 ms | 4668.278 ms | +0.95% | 79,633,004 | 72,622,119 | -8.80% |
| `microsoft/vscode-docs`, 100 commits | SlateDB | 4857.188 ms | 4809.890 ms | -0.97% | 79,760,567 | 72,545,795 | -9.05% |
| `home-assistant/brands`, 80 commits | RocksDB | 313.514 ms | 313.681 ms | +0.05% | 15,854,439 | 15,853,294 | -0.01% |
| `home-assistant/brands`, 80 commits | SlateDB | 374.780 ms | 374.387 ms | -0.10% | 15,898,061 | 15,893,791 | -0.03% |
| `wesnoth/wesnoth`, 15 commits | RocksDB | 125.481 ms | 121.232 ms | -3.39% | 4,487,302 | 4,345,582 | -3.16% |
| `wesnoth/wesnoth`, 15 commits | SlateDB | 125.817 ms | 124.612 ms | -0.96% | 4,484,039 | 4,340,966 | -3.19% |
| **aggregate** | | **10421.257 ms** | **10412.081 ms** | **-0.09%** | **200,117,412** | **185,601,547** | **-7.25%** |

All final-tree checks passed. VS Code materialized 97 unique LFS objects with
42,011,887 logical payload bytes before and after; Brands and Wesnoth
materialized none. LFS payload is a replay accounting category, not a CAS or
plugin API concept. These were initial materializations without a verified
base edit and therefore used the normal full-blob path. The table is the
complete Lix database footprint and includes Lix's physical representation of
those payloads.

On VS Code/SlateDB, delta manifests replace 1,484 whole-file chunks. Immutable
chunk value bytes fall from 74,441,216 to 67,267,601 while encoded manifest
bytes rise from 194,970 to 391,995. Wesnoth replaces 25 whole-file chunks;
immutable chunk value bytes fall from 3,485,355 to 3,339,981. Brands is the
media-heavy control and stays flat.

## Matched Git and hot-path benchmark

The deterministic Markdown corpus is 524,721 bytes. Before and after use the
same public SQL writes and release configuration. Duration rows are medians;
the unprofiled hot loop uses 500 edits.

| operation | before | after | delta | Git after |
| --- | ---: | ---: | ---: | ---: |
| hot byte edit | 3.231 ms | 2.187 ms | -32.31% | 5.751 ms matched edit median |
| cold materialized read | 0.722 ms | 0.573 ms | -20.64% | 1.260 ms |
| semantic edit | 6.052 ms | 5.932 ms | -1.98% | 5.751 ms |
| unrelated-entity merge | 6.744 ms | 6.714 ms | -0.44% | 10.326 ms |
| initial import | 32.534 ms | 31.944 ms | -1.81% | 5.552 ms |

The byte-edit path is 3.10x faster than Git and the cold materialized read is
2.20x faster. This is a material improvement but does not claim the broader
10x target: initial import remains 5.75x slower than Git, semantic edits are
3.1% slower, and the Wesnoth storage window remains above the requested 2x
Git footprint.

The before Samply profile is
`bench-artifacts/git-comparison/profiles/markdown-byte-write-before.json.gz`;
the after profile is
`bench-artifacts/piece-table-cas-delta-512/profiles/markdown-byte-write-after.json.gz`.
The before profile attributed about 43% of sampled hot-path time to Zstd
compressing the repeatedly rewritten whole CAS chunk. The after loop is
2.211 ms/edit under sampling and stores only the bounded delta manifest.

Both RocksDB and SlateDB passed the eight-operation tracked-state CRUD smoke
matrix (insert/read/update/delete, bulk and keyed). The local Codspeed
compatibility runner reports correctness but not statistically useful timing;
replay and the matched benchmark above are the performance checks.

[git-pack]: https://git-scm.com/docs/gitformat-pack
[rocks-blobdb]: https://github.com/facebook/rocksdb/wiki/BlobDB
[rocks-key-layout]: https://github.com/facebook/rocksdb/wiki/Basic-Operations#key-layout
