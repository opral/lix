# ForkTree Stage-2 authority deletion budget — corrected

Status: frozen test/static evidence only. This package changes no production,
cursor, adapter, or ForkTree source.

## Immutable inputs

- baseline main `b5e78190f49cab5de7bb19b6f967706c214363b6`, tree
  `c913465505bc773d21a6e2804530287ee937a3f1`;
- approved unwired Stage 1 `138b55e1de90806c380ad27b2b349f4c66a1387f`,
  tree `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`;
- later main `e8713ed191e05d29c44dbc8e7ce1d6b1a11695e7`, tree
  `ce241a0af016cadcb0c21d2d754eb3d4291cf79c`;
- landed current main `803d19ec0b67fb4b759aceab7ceb74650d9d894f`,
  tree `2ae6ffd8faef595ca9bf2e60447ef31a8922b92f` (the modeled e871 +
  #1260 tree matched exactly);
- landed #1258 map: 21 production paths and 39 physical CAS/retention
  symbols, all still mechanically verified.

Every input is deliberately pre-cut and therefore red. These counts calibrate
the first runnable Stage-2 zero-residue gate; they are not accepted failures.

## Exact corrected budgets

| Input | legacy spaces | owner/codec tokens | deleted modules | old cursor IDs/patterns | FileStorage owners | SQLite tokens/package | required Stage1 owner occurrences | executable semantic facades passing | findings |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| b5 | 42 / 699 | 151 / 2,585 | 23 / 23 | 7 / 655; 13 / 19 | 4 / 24 | 5 / 38; present | 17 / 0 | 1 / 19 | 295 |
| Stage1 138b | 42 / 657 | 151 / 2,210 | 23 / 23 | 7 / 619; 13 / 19 | 4 / 24 | 5 / 38; present | 17 / 524 | 1 / 19 | 233 |
| e871 | 42 / 702 | 151 / 2,594 | 23 / 23 | 7 / 655; 13 / 19 | 4 / 24 | 5 / 38; present | 17 / 0 | 1 / 19 | 295 |
| main 803d | 42 / 702 | 151 / 2,594 | 23 / 23 | 7 / 655; 13 / 19 | 4 / 24 | 5 / 38; present | 17 / 0 | 1 / 19 | 295 |

The corrected 42nd durable space is
`BINARY_CAS_MUTATION_EPOCH_SPACE`; it contributes six of b5's exact 699
legacy-space occurrences. The first runnable cut requires zero occurrences of
all 42 spaces, all 151 physical owner/reader/writer/codec symbols, all old
cursor identifiers/patterns, all four FileStorage owner types, all SQLite
selectors/dependencies, and absence of all 23 superseded modules.

## Storage and cursor boundary

SQLite is not a retained adapter or an acceptance target. Stage 2 must delete
`packages/sqlite-storage`, its workspace dependency/features, and every
runtime/test selector. The custom CLI `FileStorage` implementation and its
`FileLix`, read, and write types are also deleted. Normal CLI local open/init
must use RocksDB; explicitly selected SlateDB remains. There is no SQLite or
FileStorage compatibility reader and no migration gate.

The cursor scan covers every Rust source under retained `packages`, including
Lix, CLI, local-filesystem/file support, Memory, RocksDB, SlateDB, benches,
examples, integration support, and workspace tests. Only this oracle's own
negative probes are excluded. `packages/sqlite-storage` is excluded from
adapter conformance because physical deletion is checked separately.

The post-cursor allowlist is limited to `ScanChunk`, `BeginScanOptions`,
`ScanOrder`, `ScanCursor::next_page`, doc-hidden `StorageScanSource`, and direct
`StorageRead`/`StorageAdapterRead::begin_scan`. `StorageRead::scan`,
`ScanOptions`, `StorageScanOptions`, `ScanPlan`, `ScanPlanCursor`,
`resume_after`, `scan_resume_after`, `expected_resume_after`, `page`,
`first_page`, hidden Slate resume/cache state, and adapter reconstruction loops
are blockers. Exclusive `KeyRange.lower = Bound::Excluded(authenticated_key)`
is the only restart shape.

## Production declaration and semantic gates

The declaration ledger no longer truncates a file at the first `#[cfg(test)]`.
It masks comments/literals, removes only exact item-scoped `#[cfg(test)]`
items/methods, and continues scanning declarations and reconstruction loops
after interleaved test blocks. `#[cfg(not(test))]` and potentially production
`#[cfg(any(test, ...))]` items remain visible. Frozen ledgers contain 1,658 b5,
1,622 Stage1, and 1,669 e871/main-803d declaration rows after the TSV header.

The 19 semantic allowlist rows are executable rules, not string exceptions.
Each value/facade/relation must exist. Facades and relations must have a
ForkTree delegation, and no matching declaration/body may own raw
`StorageSpace`, write/delete operations, `StorageWriteSet`, or a legacy durable
space. The global zero-residue gate independently forbids every listed legacy
owner/codec. Thus a retained public name cannot hide a compatibility reader,
dual writer, fallback layout, or side mutation index. Pre-cut trees pass only
the authority-free `BranchOperation` value rule (1/19), as expected.

## Compiler dependency order

The standalone DAG has 21 nodes and 35 edges. The required non-runnable wave
is reader-first and writer-last:

```text
G0 -> M0 -> R0 -> R1..R7 -> W0 -> W4 -> W1/W2/W3 -> W5
W1/W2/W3/W5 -> D1; reader and writer prerequisites -> D0/D2 -> C0 -> C1
```

`W4` installs the single selector/epoch publication fence before tracked,
history/ref, upload/blob, or root publication (`W1`-`W3`, `W5`). All
publication and root prerequisites precede unified sweep. Working-diff readers
move before its writer is removed. The 42 spaces, 23 modules, old codecs,
SQLite/FileStorage, and old scan surface are deleted before residue gate `C0`;
`C0` precedes the first accepted compile `C1`. A cycle or any edge permitting
sweep before publication/root completeness fails the oracle.

## Compile-probe calibration

Exact b5 rlib SHA-256
`2217431d3abecf264b9cadb6eb69c652fd6f6646b47b61ef07456544872f5221`
still compiles both equivalent raw-space mutation and old one-shot scan, while
the new streaming probes are unavailable. Exact Stage1 rlib SHA-256
`e8c2ee1103b921598ad999a6ec0ce9b1fd6adfc75cd7bfffc1d97b225fcf5bfc`
rejects equivalent-space forgery with `E0599`/`E0423` (stderr
`2795efa59d4b1ae0052ddfc79301e0d3ab3981c376d894cd1c10aa97ca0cf450`)
but still compiles the pre-cursor old scan. Full probe matrix SHA-256:
`d974ef724a8215293eddf97be0119066bc412102778f83299f7b3cabe0f89f8b`.

On the immutable cursor successor and runnable Stage2 candidate, old scan and
continuation-field probes must reject; direct begin-scan and authenticated
exclusive restart must compile. On Stage2, equivalent-space mutation must
also reject. Every probe uses an rlib built from the exact tree under review.

## Frozen evidence hashes

- scanner source: `6fcc2e476f81f4d70227eef0d249d305ee30ba4edfad005f7fb7b73e0daf0d5f`;
- warnings-denied scanner binary: `86741026c9f8ce8b746806c51a964d2b79a1a2477e11eb1c11e465ba13221d15`;
- dependency source: `eb11912d9b8ed8b7222ee98792f00c2614373ff615d22c221f00a3ebeeb413e3`;
- dependency binary: `1f1eadf3e0c8a1aeca8e8a4fc31cb4492f988dbe12d72c3832c223e522a8ae81`;
- semantic allowlist: `1a70be6bf43e3f93bc57c0c5a8aec813757187484e3b1f4c45218621d0e79021`;
- b5/Stage1/e871/main-803d budgets:
  `0d232b5eb18cf9ad409b2623d25ddb86fa83d3c1781e978fac5b36cde96503e6`,
  `e2ae8494a2e47946c5625f00823508bc50e8cf154704f288cff61e4940e989dc`,
  `224965d21b84c6171b0440122e0a223a16c07b7141ec019a408ae8e7bf2d117c`,
  `0c00637446c057ecd2fe7717fcd61d2366fe679c2bc925eafa9d80e149db7ad7`;
- b5/Stage1/e871/main-803d declaration ledgers:
  `a6a7dbce81a6f2737474be309aed14dcddc28f28c29166ae63764be5c96baebf`,
  `17a71d6305963b7cc55ae522ce318c83031aa10b0c71d94e3742f6dd5a6740c5`,
  `90950db072d2ef516460ececa2404c0a05cd9542dec605da5216ce586e4d05bb`,
  `8144809e65f453667bba2b4b3c6dddab385cd94b81bd02e47cb7e1399470aa05`;
- b5/Stage1/e871/main-803d baseline logs:
  `5ac2e8c6ef3734d6b621694df9d0e5aee7f9bb9eb82051f7f3ff12fd2693ad3c`,
  `626d6b80e35991e0b8cf576c50eae26a6835d9c90ee1f2d1e8fade03962958f9`,
  `41f88b2aebc4333cd4ede6a7f43ff7a9a7fb86ec721ddeee036e1be5d36190c4`,
  `41f88b2aebc4333cd4ede6a7f43ff7a9a7fb86ec721ddeee036e1be5d36190c4`;
- landed #1258 verifier:
  `371f0dbe0e8423c5264f88bf33614ae2a559585643d340cd013f06e8e40db368`.

Current/proposed complexity is unchanged: current legacy publication/serving
contains multiple materialization/rebuild planes; Stage2 publication is
`O(U log_F N + Z)`, point/range reads are `O(log_F N)` / output-linear,
catalog lookup is `O(log_F M)`, selector moves are `O(1)`, upload part update
is `O(part_bytes + log_F P)`, and bounded GC is `O(S + Q + R + O)` total work
with page/pack-bounded memory. This oracle adds only linear source scanning and
does not authorize implementation or compatibility paths.
