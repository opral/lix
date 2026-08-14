# Parsed-file qualification: PR #1480 versus exact main

Verdict: **qualified NO-CUT**. The candidate preserves every checked semantic
digest, corruption/reopen control, and storage shape, but no distinct owner
offers a greater-than-20% improvement on both RocksDB and SlateDB. No
production change was made.

## Immutable production identities

- candidate: `00f65e8fdf2344ecf671c212b5c42d176859a586`, tree
  `94fb7144c43c8d9990c935bf184f202b0a5deaa0`, parent
  `24eb4237765dbbd70a9376336bd66cf0e3456c31`
- exact main: `d2c634b2aeb780aff46013ec04902fcbb5c6f846`, tree
  `d321745bf83a7e7358b038880ad40004fc888ee5`
- main..candidate full-index binary SHA-256:
  `699314d06d177a5d5eba16195687a163273db03bb1722986b0ea91339dfb8c9b`
- stable patch ID: `19669695ed5dc9d9ab40d038387492194d11f182`

The public Markdown qualification source before counter instrumentation is
byte-identical on both lineages, SHA-256
`3a26c6d664d076fd19a2a81e5fef0fce11ca38eed738741f3dc5436d75282b77`.
The byte-identical adapter-counter overlay has SHA-256
`2b7a5fb79f535e14844aa61592750829898eb9cbad68fdd77ee074fa26657af3`.
The all-plugin scale runner is also byte-identical, SHA-256
`161436990c538ea278d32eabaf71f8f6739bafcfb93815a7dae88cde20110ef7`.

## Workloads and correctness

The primary matrix used a real installed Markdown component plugin and public
`lix_file`/`markdown_node`/`lix_diff`/history/branch APIs: 3 warmups plus 10
fresh-database samples on each adapter. Its 3,226-byte file produced 105 typed
rows. It measured insert/parse, exact point, bounded range, full scan, one
semantic update (0.95% of rows), history, diff, a 17-file transaction, branch
create/switch/read, flush/drop/cold reopen, allocations, process CPU/RSS/I/O,
adapter calls/bytes, and settled disk.

All main/candidate and RocksDB/SlateDB digests match: fixture, rendered file,
point, range, full rows, 105 historical rows, one-row diff, 17-file batch, and
105 rows after cold reopen. The all-plugin scale matrix additionally exercised
real Markdown, CSV, JSON, text, and Excalidraw files at 10/100/1,000 semantic
rows per file, 10 or 30 total files (5 or 25 affected plus 5 unaffected), and
50%/83% affected-file fractions. Five fresh-database samples per cell passed
merge preview/commit agreement, semantic oracle, source isolation, idempotency,
unaffected-file preservation, corruption assertions, and cold reopen.

## Primary p50/p95 wall results (ms)

| adapter / operation | main p50 | candidate p50 | ratio | main p95 | candidate p95 | ratio |
|---|---:|---:|---:|---:|---:|---:|
| Rocks parse/insert | 47.694 | 48.884 | 1.025 | 50.117 | 56.042 | 1.118 |
| Rocks exact point | 0.605 | 0.615 | 1.016 | 0.859 | 0.911 | 1.061 |
| Rocks range | 1.324 | 1.371 | 1.036 | 1.693 | 1.839 | 1.086 |
| Rocks full scan | 1.062 | 1.168 | 1.100 | 1.471 | 1.660 | 1.128 |
| Rocks update | 4.113 | 4.775 | 1.161 | 5.497 | 6.219 | 1.131 |
| Rocks history | 1.388 | 1.397 | 1.007 | 1.758 | 2.032 | 1.156 |
| Rocks diff | 0.982 | 1.200 | 1.222 | 1.188 | 1.606 | 1.352 |
| Rocks 17-file transaction | 33.510 | 37.073 | 1.106 | 39.841 | 41.296 | 1.037 |
| Slate parse/insert | 40.515 | 41.357 | 1.021 | 42.889 | 48.030 | 1.120 |
| Slate exact point | 0.546 | 0.565 | 1.034 | 0.608 | 0.740 | 1.217 |
| Slate range | 1.275 | 1.290 | 1.012 | 1.446 | 1.879 | 1.299 |
| Slate full scan | 1.088 | 1.071 | 0.984 | 1.279 | 1.647 | 1.287 |
| Slate update | 4.797 | 4.656 | 0.971 | 5.919 | 6.714 | 1.134 |
| Slate history | 1.468 | 1.316 | 0.896 | 1.856 | 1.643 | 0.885 |
| Slate diff | 1.170 | 1.168 | 0.998 | 1.471 | 1.639 | 1.114 |
| Slate 17-file transaction | 38.688 | 39.869 | 1.031 | 42.478 | 51.336 | 1.209 |

Settled bytes remain effectively equal: Rocks 12,290,009 main versus
12,238,029 candidate (0.996x); Slate 12,247,259 versus 12,195,650 (0.996x).
Allocation medians are within about 1% except history, where the candidate is
about 10% lower on both adapters.

## Scale result and attribution

At 1,000 semantic rows/file and five affected files, candidate/main merge p50
is 108.472/100.475 ms (1.080x) on RocksDB and 109.319/101.933 ms (1.072x) on
SlateDB. At 100 rows and 25 affected files it is 27.462/25.901 ms (1.060x)
and 30.875/30.073 ms (1.027x), respectively. None of the eight scale cells
shows a cross-adapter candidate win above 20%; settled bytes remain within 2%.

Adapter instrumentation disproves backend amplification as the owner. For a
semantic update, main to candidate get batches fall 97 to 89; for the 17-file
transaction they fall 710 to 642. Write batches stay 19/24, puts stay 30/about
372, and logical write bytes stay about 14.5/141 KiB. The dominant 1,000-row
merge spans are instead plugin work in
`packages/lix/src/transaction/context.rs`: `plugin_selection`,
`plugin_semantic_actor_cold_open`, and `plugin_semantic_render`. Combined they
account for roughly 91--99 ms of transaction planning, with candidate CPU about
9% above main on both adapters. This is required per-file plugin
classification/instantiation/rendering, not a distinct storage hard-cut seam;
the existing transaction-local actor publication path already prevents a
second authority or persistent cache. A safe batch ABI/module lifecycle change
would exceed this lane and is not evidenced to clear the cross-adapter 20%
gate.

## Evidence hashes

- main primary summary/log:
  `d585c9021709e8d670d074eb2fc7f8609ad847e75ce7d7f5c5d2064de34667bf`
- candidate primary summary/log:
  `213d4eda6012113a73c3cedcbee9095281676a98629e93b891ef5baa80400452`
- primary comparison:
  `62db1e6dde970144f574f602a71a9cc99ae9df308ca8bb2831d0876ad832fc66`
- main five-sample scale raw/summary:
  `0b693b2e6c44d37effb3b5ffdc16d45c6853cb37dfddb939ae081c83d290f975` /
  `f974ac283fb282665e145e9566bfb9b9ce652171a8bab9186f3a4fd67b486a4c`
- candidate five-sample scale raw/summary:
  `111c9d2dc922181480b19ab1e91cffe8e03f00f73d21d6ca99a3f255ff87df25` /
  `59e89ccb37232be1c70e804b61bd1072ec250fb31b310b4f0f0fbbf4cc175ed2`
- scale comparison: `7a0bf1e253097563d03c7b79c961ea837d50414b71fc192017ee2ac7ff745cf0`
- adapter-call comparison: `b310a1ac091000b99477945562cafa752396e26a8671821f7bb4d6263841aff0`
- top phase attribution: `0b7373f88da354cf99daf535a773ae0cf13de25a14feccd2dde5b1e6d3a06abf`
- main/candidate primary binaries:
  `663df7339eedc7862c210d2b18e225f4da6429b80f319d838a7f2d4257e06110` /
  `16799240ac5e701d0b9c6304454532072e8501df2e5d8856299989fed2e72429`
- main/candidate counter binaries:
  `5c4f8014ca0c7bf0028425caf3e3d3a58a9745f4b423a7757f33e86f80a4e2ca` /
  `c9fef42d88aa0db6e7de66e23b6b546991ef02ba535ae2b4bcaa85676e8f725c`
- main/candidate scale binaries:
  `f543063df02f4390723ee3b9e60d6255fc653069cc067082d190884950c33900` /
  `3e1a5a1fc2506239224a281010bdd2c278b45db6606bf11f41531f0322239ed8`

Each cell was capped at 1,200 seconds; no cell reached the cap. Free disk at
completion was 369 GiB. No production, public API, fallback, cache authority,
or synthetic workload path was added.
