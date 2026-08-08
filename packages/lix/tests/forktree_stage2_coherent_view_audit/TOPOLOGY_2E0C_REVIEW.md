# Stage 2 batched-topology source verdict

Verdict: **BLOCKER**. Read-only source review; no production source was edited or built.

## Immutable provenance

- Candidate: `2e0cea1b91558179e6ed90847bc8b04b23de246f`, tree `334d6b2622f156e65ba7835058cb6d8e7c240181`.
- Parent: `2f60fcfc46f71b87d71e8cd74591576a98dec4e5`.
- Parent-to-head full-index binary diff SHA-256: `c25e9fb8b2366c9d170b6521423997c1ccd5d1f0955a48d004fe2940751c52a6`.
- a12-to-head full-index binary diff SHA-256: `cfa2693c3f6509920dd07b5628e9ebf54bd156149b6a704b91ccb4511209f74e`.
- Stable patch ID: `1a4c72779def7ca5f453d0168f3528056f00f705`.
- Scope is exactly four files, `+458/-54`:
  - `packages/lix/src/commit_graph/context.rs`, blob `9207f1d45732b85caee1ca5ed3f07c67824858d0`.
  - `packages/lix/src/forktree/mod.rs`, blob `fe69da9e06f3fe14f8c554a9da0baa2a85d20a54`.
  - `packages/lix/src/forktree/serving.rs`, blob `bc53e6a206bd517b8d83a0a9e1c3810cf69fb8ad`.
  - `packages/lix/src/forktree/tests.rs`, blob `182d0f5b156c69228219d94b549a00f9f8455e21`.

## Blocking snapshot-lifetime seam

The duplicate-parent implementation is correct in its current sole caller, but its source API does not bind cached authenticated envelopes to that caller's immutable read:

- `CommitTopologyReadCache` is a standalone `pub(crate)` value with no read lifetime or snapshot identity (`forktree/serving.rs:127-140`).
- `load_commit_topology_batch` accepts an arbitrary `&R` and a separately supplied `&mut CommitTopologyReadCache` (`forktree/serving.rs:367-374`).
- The ForkTree facade reexports both crate-wide (`forktree/mod.rs:30-36`) and names the cache in its facade contract (`forktree/mod.rs:82-104`).

The current `CommitGraphStoreReader<S>` embeds `store` and `topology_cache` together (`commit_graph/context.rs:59-70`) and correctly passes those exact fields (`commit_graph/context.rs:101-129`). Nevertheless, another internal caller can reuse the same positive envelopes with a different `StorageRead`. A comment saying the cache is reader-local does not compiler-enforce snapshot binding. This fails the requested cache lifetime/one-view source invariant before the blob reader can be admitted.

The cache itself stores only positive decoded envelopes and resolved topologies. It does not store an error or `None`. Partial positive envelopes can survive a propagated error, but every subsequent resolution and catalog back-edge is revalidated on the supplied read. Separately, the pre-existing `CommitGraphStoreReader::node_cache` still records `None` for a requested absent commit; that is deterministic within its immutable read but is literal negative caching if the contract applies to every graph-reader cache.

## Corrected shared-parent path

The original duplicate-parent blocker is fixed:

- `requested_objects` retains exact batch order (`serving.rs:376-400`) and output is emitted in that same order (`449-458`).
- Requested object IDs and the union of immediate parent ObjectIds are `BTreeSet`-deduplicated before exact batch loads (`377`, `404-422`).
- Parent catalog back-edges are validated once per unique parent identity (`424-447`).
- Positive decoded parent envelopes remain available for later traversal; resolvable parent nodes seed the existing graph node cache (`460-486`, `context.rs:119-128`).
- `A -> P`, `B -> P`, `P -> G` regression counts the actual P, G, and member ObjectIds (`forktree/tests.rs:983-1185`). It asserts one retained read, exactly one P load, one G load, zero member loads, topology success after deleting P's semantic member, and fail-closed member history.

There is no new `begin_read`, Change/member hydration in topology, fallback, legacy module/space resurrection, or benchmark/model substitution. Semantic history still uses `load_commit_member_records`, which authenticates Commit membership, Change Object identity, ChangeCatalog owner/ordinal, and reverse membership.

## Frozen scanner

The scanner source is unchanged at SHA-256 `fd4894f6a71606ea732f944e297e51a0eaadbeff6811518516b973d01795a4ec`. Its strict candidate run has canonical evidence digest `aa8d5570f9e655b72f9d281653a309fee63357a137903ed48d02a1ecb5f6f2e1` and combined stdout/stderr SHA-256 `d30177dfbcb21920220d3fa34429309d61980502e5fb72e444614287f9aa0794`.

Its four reds are conservative token-policy findings: it requires literal `CoherentView` in commit graph, treats public `crate::changelog` ID/codec names as payload access, expects the removed zero-parent test symbol, and recognizes `load_change` rather than `load_commit_member_records`. Manual source inspection resolves the duplicate-load, zero-member, and history checks positively; it does not resolve the detached cache/read API.

## Smallest correction contract

Replace the detached cache parameter with an opaque topology reader that owns one `StorageRead` and its positive cache together. Its load methods borrow that owner, making cross-snapshot reuse unrepresentable. Do not export a cache constructor or a batch function accepting a separate read/cache pair. Preserve exact requested ordering, unique parent loads, zero member hydration, and fail-closed catalog/generation/identity checks. Clarify whether immutable-view `node_cache: None` is accepted or remove it to meet a literal no-negative-cache rule.
