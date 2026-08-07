# ForkTree Stage-2 authority deletion budget

Status: test/static evidence only. No ForkTree, cursor, adapter, or other
production source is changed by this package.

## Pinned inputs

- exact current main: `b5e78190f49cab5de7bb19b6f967706c214363b6`,
  tree `c913465505bc773d21a6e2804530287ee937a3f1`;
- approved unwired Stage 1: `138b55e1de90806c380ad27b2b349f4c66a1387f`,
  tree `26a3e6ead4d690bf1fe2ebca1e2da7d597256b84`;
- landed #1258 changed production paths: exactly 21;
- landed #1258 physical CAS/retention symbols: exactly 39.

## Exact deletion budget

The first runnable Stage-2 source must satisfy all rows with disposition
`zero` or `absent`, every `present` owner/cursor row, and the separate semantic
allowlist contract.

| Class | Budget | b5 occurrences | Stage1 occurrences | Post-cut |
|---|---:|---:|---:|---|
| legacy durable spaces | 41 | 693 | 657 | zero |
| legacy owner/reader/writer/codec/format symbols | 151 | 2,585 | 2,210 | zero |
| superseded physical modules | 23 | 23 | 23 | absent |
| old cursor identifiers | 6 | 177 | 177 | zero |
| old cursor call/fallback patterns | 9 | 12 | 12 | zero |
| forgeable raw-space API shapes | 3 | 3 | 0 | zero |
| required Stage1 owner symbols | 17 | 0 | 524 | present |
| required streaming-cursor shapes | 8 | 1 | 1 | present after H4 |
| explicitly allowed semantic facades | 19 | 3,275 | 3,241 | body-switched; name may remain |

The global residue gate reports 255 findings on b5 and 194 on Stage1. These
red baselines are calibration, not accepted failures. The post-cut budget is
zero legacy findings.

The whole-module declaration ledger separately enumerates 522 b5 and 512
Stage1 production declarations. Those declarations are scoped by module: a
generic method such as `new`, `load`, or `scan` is not globally forbidden, but
the physical module containing it must be absent. This prevents false
positives without allowing any implementation from the deleted plane to
survive.

## Semantic-name rule

`PUBLIC_SEMANTIC_ALLOWLIST.tsv` contains only public behavior or facade names.
It does not allow an old storage space, persisted codec, compatibility reader,
fallback writer, side index, or mutation authority. Each allowed facade must
delegate to the sealed ForkTree owner, and the global residue scan still
examines its body.

In particular, `stage_repository_gc`,
`stage_repository_gc_with_preconditions`, and
`load_plugin_registry_at_commit` are the three #1258 semantic facade names;
all 39 #1258 physical symbols remain forbidden. `ScanChunk`, stats-only
`begin_scan`/`next_page`, and their ephemeral cursor scaffolding are allowed;
`StorageRead::scan`, `ScanOptions`, `resume_after`, `ScanPlan`,
`ScanPlanCursor`, `page`, `first_page`, hidden Slate continuation state, and
adapter reconstruction loops are forbidden.

## Compiler dependency oracle

The executable DAG has 21 nodes and 33 edges:

```text
G0 -> M0 -> R0 -> R1..R7 -> W0 -> W1..W5 -> D0/D1 -> D2 -> C0 -> C1
```

Every reader/consumer family precedes the first writer. Working-diff readers
and state/selector publication precede deletion of its writer. All writers and
the working-diff writer precede physical-plane deletion. All 41 spaces,
23 modules, exports, fixtures and old cursor residue are deleted before the
residue gate (`C0`), and `C0` precedes the first accepted compile (`C1`). A
cycle or an edge that permits compile before deletion fails the standalone
oracle.

## Compile-rejection contract

- On b5, equivalent raw object-space construction plus generic put/delete and
  the public `StorageRead::scan`/`ScanOptions` page reconstruction both compile.
- On approved Stage1, equivalent object-space construction fails, while the
  old scan probe still compiles pending H4.
- On the immutable cursor successor, the old scan and continuation-field
  probes must fail; direct `begin_scan` and exclusive authenticated
  `KeyRange.lower = Bound::Excluded(last_key)` restart probes must compile.
- On the first runnable Stage2 candidate, both old scan and equivalent-space
  probes must fail against an rlib built from that exact candidate.

No stale rlib is evidence. A source compile that succeeds while residue remains
is a failed gate, not an accepted intermediate state.

Focused b5 calibration used rlib
`2217431d3abecf264b9cadb6eb69c652fd6f6646b47b61ef07456544872f5221`:
the old-scan probe compiled to
`690c739a67eefb2d57c660d3b59ae7cc8daa22c97b53e567f363c57a60d267c7`
and the equivalent-space probe compiled to
`0566a079722955dc5eab767fa22cacd2217d8d13df2f334da0913027e977e5ec`.

Focused Stage1 calibration used the exact test-only application-oracle rlib
`3e67886952a26557f43b89e9028b210f265cdc433dddc4eb465d252510be0ff4`:
space forgery rejected with `E0599`/`E0423` (stderr
`2795efa59d4b1ae0052ddfc79301e0d3ab3981c376d894cd1c10aa97ca0cf450`),
while the pre-H4 old-scan probe compiled (binary
`4a575015e752c4dba7af1fd5fc5e68f79ad360512605350411964b6bea2eb7d1`).

## Frozen artifact hashes

- scanner source: `97b37217350f28e9e6b2bca14a736fe0a44d213dc5ac25184973c6602e6bc4bd`;
- warnings-denied scanner binary:
  `a4f60ab5583d7d4206cf6a8df51322a04dc550ca31ba3c1ee9a10020b1d436d9`;
- b5 budget: `0e4267e499533fd55e5b2c4a5c635772aab51fc3b85e4e12ef5c2a92269ca27e`;
- Stage1 budget: `c21a1f34247440d035f9d6114f102b026edb523ca442977b64c1841731e6980d`;
- b5 deleted-module definitions:
  `231e37d9fd7974c8e87f6cc6446c2d7559e06e70070a99f844b9be7a77649744`;
- Stage1 deleted-module definitions:
  `4ed22e54c04afc212a84bc05a7fef22f5e9f72d7bda4de31aa99c820963947ac`;
- semantic allowlist:
  `97812011963036d32d78c8805d586f26d056a0a8af8b8ce57e11df796e942a64`;
- dependency source:
  `74176627cf19ed116da69cb5bb8a89208c872f21dab4ecb9b53aaced0c032152`;
- dependency binary:
  `0e524a17f8282593905392b13210ac5613f5334d24b3a140e55ee1d26fbcf7bc`;
- b5 baseline log:
  `fa38ca30df08d030ee40a79783a583e23dd01f2ff30c934f55e0d48e4f77b267`;
- Stage1 baseline log:
  `d25a20fca7dc9d0bee82732002e1b5858d5ae4beb2c3198299997406044ee57e`;
- landed #1258 verifier:
  `371f0dbe0e8423c5264f88bf33614ae2a559585643d340cd013f06e8e40db368`.
