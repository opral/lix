# ForkTree Stage-2 post-#1260 authority addendum

Status: read-only source/static audit. The frozen `7ea488b` scanner was applied
unchanged. No production source, PR, branch, or scanner implementation was
modified for this review.

## Exact prospective identity

- exact current main: `e8713ed191e05d29c44dbc8e7ce1d6b1a11695e7`,
  tree `ce241a0af016cadcb0c21d2d754eb3d4291cf79c`;
- approved SQL-write head: `7061aad7f4b14e611b32bbe5493f39253b826378`,
  tree `d41598c18afae0b6a9c675fb8be3b263000da67a`;
- merge base: `4763408467d265b288a124e24b1d47be423f5d17`;
- clean prospective merge tree:
  `2ae6ffd8faef595ca9bf2e60447ef31a8922b92f`;
- main..prospective full-index binary diff SHA-256:
  `fa74c557636e14493a937a6e46dc77c26acb3f0938659eb93be64633956c951d`;
- stable patch ID: `30715df6569090e30c4520bf3e055bb67ff74049`.

The prospective diff is nine paths: eight `packages/lix/src/sql2` files and
`packages/rs-sdk-tests/tests/e2e.rs`, `+520/-387`. It has zero path overlap
with the 23 Stage-2 deletion modules, the 21 landed-#1258 production paths, or
the approved Stage1 production delta.

## Residue result

The prospective tree has exactly 255 pre-cut findings, byte-identical to exact
e871 main:

| Class | Items | Occurrences |
|---|---:|---:|
| legacy owner/reader/writer/codec | 147 | 2,594 |
| legacy durable space | 41 | 696 |
| superseded module | 23 | 23 |
| old paginated scan | 9 | 189 |
| unsealed raw owner | 3 | 3 |
| missing Stage1 owner | 17 | 0 |
| missing final cursor shape | 15 | 0 |

Prospective residue log SHA-256:
`44805345a65c71debf573428d9d7ef2857a349c869a1ac2b7e6901938bc1540b`.

The complete prospective budget has 278 lines and SHA-256
`ae515486658a3b2f2d8a197107fb3bc1f69464183df126a66c075379f2cd45fe`.
Compared with exact e871, every enforced row is byte-identical. The only
budget delta is the allowed `SessionContext` occurrence count, `144 -> 145`.
The enforced-budget delta is empty, SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

The prospective deleted-module ledger has 533 declarations (534 lines with
header), SHA-256
`2d477ddfa1eb7cd1f3ea175d5219a113224e3c100115a6662f67d5926e6fd1b6`.
Its module/kind/symbol set is byte-identical to e871. Differences in the fourth
column are only repository-wide occurrence counts of generic names such as
`new`, `get`, `schema`, and integer decoder helpers. The symbol-set delta is
empty, SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

Relative to the older pinned b5 budget, main itself added authenticated branch
control and plugin-checkpoint codec helpers. Those account for the increase
from the b5 definition ledger and are not caused by #1260. They remain covered
by the existing branch-control/plugin space, module, and explicit owner-token
deletion rules.

## Stage-2 disposition

**No deletion-list or compiler-order change.** #1260 adds no legacy space,
codec, reader, writer, old scan path, direct storage mutation call, or durable
authority. The 41-space/151-symbol/23-module deletion budget and the 21-node,
33-edge reader-first/writer-last dependency DAG remain exact.

The semantic integration boundary becomes more precise:

- `SpecTableProvider` is read-only and must remain free of mutation authority;
- transaction-local `WriteTargetRegistry` maps a bound table name to one
  `SpecWriteTarget` and is dispatch state, not persisted state or an index;
- `SqlWriteSession`, `SqlWriteContext::write_targets`, and
  `SqlWriteContext::into_physical_target` keep that capability outside the
  public DataFusion provider;
- `SpecWriteTarget` continues to own SQL normalization, `RETURNING`, conflict,
  and surface-specific planning, while actual durable publication must flow
  through the one typed ForkTree transaction owner;
- Stage-2 supplies the typed point/range/diff/history/publication capabilities
  beneath this boundary and must not restore `TableProvider::{insert_into,
  update,delete_from}`, downcast-based write helpers, or a second registry.

These names are legitimate transaction-scoped SQL semantic facades. They do
not need scanner exceptions because none matches a forbidden physical token.
The prospective SQL additions contain zero new `StorageSpace`, generic
put/delete, or begin-write calls; the added-authority search is empty with
SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

## Conflict-sensitive first-runnable gates

Keep the existing Stage-2 sequence. At the SQL boundary additionally require:

1. no write methods on `SpecTableProvider` and no provider downcast to recover
   a write capability;
2. exactly one transaction-local `WriteTargetRegistry`, unavailable to a
   physical target through `into_physical_target`;
3. unknown/read-only surfaces fail closed rather than falling back to
   DataFusion DML;
4. insert/update/delete/upsert, `RETURNING`, diff commands, omitted columns,
   branch scope, and rollback retain their public results while staging only
   through ForkTree;
5. the unchanged residue scanner reports zero before the first accepted
   compile.

Conclusion: **NO Stage-2 authority/deletion blocker and no new dependency
cycle.** Preserve #1260 as the sole SQL write-dispatch owner during the
non-runnable ForkTree wave.
