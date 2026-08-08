# Stage 2 owned-topology-reader source verdict

Verdict: **SOURCE APPROVE** for the topology milestone. Read-only source review; no production source was edited or built. Blob-reader qualification remains a separate successor gate.

## Immutable provenance

- Candidate: `af7899f41c489fe763ce1a64c5468083570979e2`, tree `da097bd739b50629ea39b155d4fa9efc870654e0`.
- Parent: `2e0cea1b91558179e6ed90847bc8b04b23de246f`.
- Parent-to-head full-index binary diff SHA-256: `942d05f6c92f89e6c32c3b706c82c4e506e498263b5798c92eb2af607a219587`.
- 2f60-to-head full-index binary diff SHA-256: `f2a7b218c2e39143640171da2aac2e4a322e5dbd5baa6f907d9750a0afd75300`.
- a12-to-head full-index binary diff SHA-256: `734d02bfe332e4f8384301de243d85248b639aec0edeffac48b3f56a4ec271e5`.
- Stable patch ID: `6ed511438fe08387ea40a5b6861f7db9f3544764`.
- Parent delta is exactly four files, `+69/-31`:
  - `packages/lix/src/commit_graph/context.rs`, blob `6e0db738f4b7ccf3986a544ac5536530836b19fb`.
  - `packages/lix/src/forktree/mod.rs`, blob `aa8effa821920a4c98578f649c3ee1bba9cac61f`.
  - `packages/lix/src/forktree/serving.rs`, blob `e53a2e785bd21a8f073721c26e9015a433997109`.
  - `packages/lix/src/forktree/tests.rs`, blob `11684bf2d0fac26c7c9d2123acf6cc19841bb6dd`.

Remote `origin/codex/forktree-stage2-milestone3e-topology-owned-reader` resolves exactly to the candidate head and tree.

## Snapshot and cache ownership

- `CommitTopologyReadCache` is private to `forktree::serving` and has no exported constructor (`serving.rs:127-140`). It contains only positive decoded envelopes, positive ObjectId-to-CommitId bindings, and positive resolved topologies.
- `CommitTopologyReader<R>` owns `read: R` and that private cache in one value (`serving.rs:147-152`). Its complete API is `new(read)`, borrowed `read()`, and `load(&mut self, ids)` (`154-175`). There is no read replacement/extraction, cache accessor, detached cache argument, or cross-view reuse seam.
- `load_commit_topology_batch` is private (`serving.rs:395-401`), and neither it nor the cache appears in the ForkTree facade. The crate-visible owner type is generic over and owns the exact `StorageAdapterRead`.
- `CommitGraphStoreReader<S>` owns one `CommitTopologyReader<S>` (`commit_graph/context.rs:43-65`). Topology loads, scans, semantic Commit reads, and semantic member reads all borrow that same owned read. No changed path calls `begin_read`.

## Positive-only and exact-batch behavior

- `node_cache` is now `HashMap<CommitId, CommitGraphNode>`, not `Option<CommitGraphNode>` (`context.rs:59-65`). Requested misses remain `None` in the returned exact batch but are never inserted (`101-134`). Errors are propagated and never represented in either cache.
- The private topology cache mutates only after successful content-addressed Commit decoding. `resolved` mutates only after identity, generation, parent uniqueness, and required parent-envelope checks succeed. Partial values surviving an error are positive authenticated envelopes on the same inseparable snapshot; the error itself is not cached and required catalog back-edges are retried.
- Requested positional correspondence is preserved: `requested_objects` is appended in input order, missing entries retain their exact slot, and `requested` is emitted in that same order. The graph reader de-duplicates uncached loads but reconstructs the public `ExactBatch` in original caller order, including duplicates.

## Parent and semantic authority

- Requested Commit objects and the union of immediate parent ObjectIds are deduplicated before exact batch reads. Parent catalog back-edges are validated once per unique parent. Resolvable parents seed the positive node cache; deeper decoded envelopes remain in the snapshot-bound owner so traversal never reloads the current parent.
- The non-vacuous `A -> P`, `B -> P`, `P -> G` regression remains unchanged from the accepted correction (`forktree/tests.rs:997-1199` at this head). It counts exact ObjectIds and asserts one P load, one G load, one retained read, zero member loads, and later P/G traversal without reload.
- After deleting P's member Change object, sibling topology still succeeds while `load_commit_member_records(P)` fails closed. Semantic history continues to authenticate ordered Commit membership, Change Object identity, ChangeCatalog owner/ordinal, and the reverse member edge.

## Deletion and scanner result

No scanner-tracked legacy owner module or durable space reappears relative to cbe. The parent delta adds no benchmark/model implementation marker, fallback, compatibility reader, selector, persisted cache, or blob path.

The frozen scanner source remains SHA-256 `fd4894f6a71606ea732f944e297e51a0eaadbeff6811518516b973d01795a4ec`. Its exact strict run has evidence digest `83eaf854a7c7ed521e5c04811a66cac41c0c52eebca0fdd499721714c715e5f0` and combined stdout/stderr SHA-256 `d2f04b7a3e8557660ae1541f048d833deb4f6162b4a9d71c22f916d34de72168`.

The unchanged scanner still emits four documented conservative token-policy reds: literal `CoherentView` naming, public `crate::changelog` ID/codec namespace, the removed zero-parent test symbol, and direct `load_change` recognition rather than `load_commit_member_records`. Targeted source inspection resolves all four: the owned `StorageRead` is stronger than a token match, topology decodes no Change/member payload, the shared-parent regression is non-vacuous, and semantic member/back-edge authentication is explicit.
