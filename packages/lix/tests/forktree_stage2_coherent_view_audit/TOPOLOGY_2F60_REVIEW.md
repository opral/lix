# Stage 2 corrected-topology source verdict

Verdict: **BLOCKER**. Read-only static review; no production source was edited or built.

## Immutable object

- Base semantic/cursor control: `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`, tree `9a705d36392e88d8f5f363b2b23d373deec3321d`.
- Scanner baseline: `cbe48835f6f07a21e0babf1ba16652a0c6b8a214`, tree `36ffe0ff867cd31bf52263675de2d16fc54e9b4f`.
- Candidate parent: `43360bd74a618bc4a3b8ebe92f6584414f1916a2`.
- Candidate: `2f60fcfc46f71b87d71e8cd74591576a98dec4e5`, tree `f938ebb380f874dbc947bef2ccec02506477b659`.
- Parent-to-head full-index binary diff SHA-256: `ba843769a6a518eb2ec0a0b88d4bc53d89ff17a169f61b87f1d3dc28f9114937`.
- a12-to-head full-index binary diff SHA-256: `739ba0106084ddae01291d34f699b74583db3d809210720d89f2b5a85ac1994c`.
- Parent-to-head stable patch ID: `e30ddfb7c0fbf34c2ed3fa921c927912038204e6`.

The parent delta is exactly six production files:

| Path | Candidate blob |
| --- | --- |
| `packages/lix/src/commit_graph/context.rs` | `efc8ca9d18b88a9f1a5c2c4e87f531a3169e0d78` |
| `packages/lix/src/commit_graph/types.rs` | `045db30ca3650f17bc5e18d88bb1d31a171f6cb9` |
| `packages/lix/src/forktree/mod.rs` | `aecd5aabd7a11e307abf4f08cbee5149eac412a2` |
| `packages/lix/src/forktree/serving.rs` | `607962d62d4af3bc425dc725e916c09bed212ead` |
| `packages/lix/src/forktree/tests.rs` | `e4d355d25776fd6969fa0fc0059d520cecf07913` |
| `packages/lix/src/forktree/view.rs` | `17d3caa324872d9cd485a3af3243b4fc497df685` |

## Blocking causal path

`CommitGraphStoreReader::load_nodes` de-duplicates only the requested public CommitIds and inserts only those returned topologies into `node_cache` (`commit_graph/context.rs`, lines 99-123). Each `load_commit_topologies` item then calls `validate_commit_topology` (`forktree/serving.rs`, lines 330-360). That validator loads and authenticates every direct parent Commit object in one child-local `load_object_map` (`forktree/serving.rs`, lines 559-612), but it returns only parent IDs and generation and does not expose the decoded parents for cache insertion.

Therefore a child `C -> P` loads `P` while validating `C`, then the graph traversal loads `P` again because `P` is absent from `node_cache`. Two siblings `A -> P` and `B -> P` in one requested batch each load `P` independently because the `BTreeSet` is local to each validator call. Object identity remains authoritative and authentication is fail-closed, but the explicit no-duplicate-parent-object-load contract is not met.

The added regression `commit_topology_never_hydrates_member_changes_and_member_history_fails_closed` uses a generation-one commit with no parents and counts only one forbidden member ObjectId. It proves zero Change-member hydration, but cannot observe either duplicate-parent case. The existing `node_cache` and requested-ID `BTreeSet` do not close that gap.

## Other authority findings

- `open_coherent_view` contains exactly one `begin_read`; `open_coherent_view_on_read` contains none and owns the exact `R` in `CoherentView<R>`.
- The topology call closure contains no nested `begin_read`. All selector, catalog, Commit-object, parent, and tree reads shown in the correction receive the same caller-owned `StorageAdapterRead`.
- The scanner's `topology-same-coherent-view` red is conservative naming: `CommitGraphStoreReader<S>` stores the caller's immutable `StorageAdapterRead` rather than the `CoherentView` wrapper. No refresh was found in the changed closure.
- The scanner's `crate::changelog` topology hit is a type/codec namespace hit. `load_commit_topologies` and `validate_commit_topology` decode Commit objects and parent/catalog back-edges but do not decode `ChangeObjectV1` or member payloads.
- Semantic history calls `load_commit_member_records`; that path authenticates ordered Commit membership, Change objects, ChangeCatalog owner/ordinal, and the reverse member edge in `semantic_change_record`. The scanner expected a differently named direct change loader, so its history red is conservative rather than an observed authentication omission.
- No scanner-tracked deleted owner module or legacy durable-space definition reappears relative to cbe. No benchmark/model substitution marker occurs in changed production files.

These three conservative reds do not override the independent duplicate-parent blocker, and the frozen gate's all-green decision rule is not satisfied.

## Smallest correction contract

Use one exact-batch topology operation that authenticates each requested Commit object and every unique parent object once, returns enough authenticated parent topology to seed `CommitGraphStoreReader::node_cache`, and preserves ObjectId/CommitCatalog as the sole authority. Add a counting regression with at least two children sharing one parent and a deeper parent edge; require one physical load per unique parent, zero member-object reads, one retained `StorageRead`, and unchanged fail-closed semantic-history owner/ordinal checks. Do not add a cache beyond the existing per-reader node cache, another selector, or a compatibility path.

## Exact command

```sh
python3 packages/lix/tests/forktree_stage2_coherent_view_audit.py \
  --repo . \
  --baseline cbe48835f6f07a21e0babf1ba16652a0c6b8a214 \
  --target 2f60fcfc46f71b87d71e8cd74591576a98dec4e5 \
  --profile topology --strict
```

The strict run exited 1 with canonical scanner evidence digest `f2a3e156f2e287db11207275b5b7f8cac3bb133878888f2d7ea97790e2094c7c`.
