# Authenticated blocked multimedia tree: research result and implementer contract

Status: **model accepted for implementation research; not production-ready code.**
No production path is edited by this change.

## Exact objects

- Exact current-main base: `f77f5b9e2ff582f749d1c487d95e6c0e8e4d3662`
- Base tree: `597b98f80dad062b4c0b244f2e59fa489a9d4ce9`
- Accepted stage-one ForkTree head: `4b7b3aa25ebed5f022ed258c172c27e4dc64753d`
- Accepted stage-one ForkTree tree: `5cafd24b60112220e86c5bccaf5fb382416f2666`
- Accepted density prototype: `bc82385ec42b1789018fbd1213f637c19104a02c`
- Accepted prototype `RESULTS.md` SHA-256: `983a26129f55021c825e6739607ef262d7f0487b7324c97043d46aa5c5f5f7b2`

The model uses the real RocksDB and SlateDB adapters, but its spaces and codecs
are benchmark-only. Setup, deterministic payload creation, and seed flush are
excluded from publication timing.

## Dominant term, ceiling, and complexity

Let `N` be logical bytes, `L=1 MiB` the leaf size, `C=ceil(N/L)` leaves, `E`
edited bytes, `K` touched leaves, `F=64` internal fanout, `D=ceil(log_F C)`,
and `Q=8` the bounded leaf-authentication batch.

Current fixed-manifest publication computes identity over all content and
rewrites `C` references: CPU `O(N+C)`, memory dependent on the caller's full
payload, and writes `O(KL+C)` bytes/metadata. The accepted stage-one flat
ForkTree manifest still has `C` references and a separate whole-content digest,
so identity remains `O(N+C)` even though chunk objects are immutable.

The proposed blocked tree performs `O(KL + P*F*D)` authentication/hash work,
where `P` is the number of touched paths, uses `O(QL + P*F)` transient memory,
issues `O(D + ceil(K/Q))` bounded backend requests, and writes only `K` leaves
plus copied path nodes. For contiguous edits this is effectively
`O(KL + ceil(K/F)*F*D)`, independent of untouched payload bytes.

The perfect-elimination ceiling is every untouched payload identity pass and
every untouched flat-manifest reference rewrite. The measured cells touch:

| Size/edit | Leaves touched | Payload avoided | Old+new object hash input |
|---|---:|---:|---:|
| 64 MiB / 1% | 1 / 64 | 98.4% | 2.102 MiB |
| 64 MiB / 10% | 7 / 64 | 89.1% | 14.685 MiB |
| 512 MiB / 1% | 6 / 512 | 98.8% | 12.589 MiB |
| 512 MiB / 10% | 52 / 512 | 89.8% | 109.065 MiB |

`hash_payload_bytes` counts freshly identified successor leaf payload;
`hash_object_bytes` counts exact encoded old/new leaf and internal-object input.
Transport verification is outside this publication model and remains honestly
`O(N)` if the public transport requires a whole-stream digest.

## Final measurements

Values are medians after seed/flush. Wall/CPU are microseconds. `Alloc` is
transient allocated bytes. Reads are logical value bytes returned to the model;
writes are logical key+value bytes. Slate physical read/write bytes are adapter
counters after each settle. Disk is the first settled publication delta.

| Backend | Size/edit | Current wall | Flat FT wall | Blocked wall | Wall gain vs current | Blocked CPU | Current / blocked alloc | Calls / logical read | Logical write / first disk |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| RocksDB | 64/1% | 7,968 | 15,224 | 1,592 | 80.0% | 1,906 | 3.152 / 3.159 MiB | 2 / 1.002 MiB | 1.003 / 1.019 MiB |
| SlateDB | 64/1% | 7,685 | 14,583 | 657 | 91.5% | 693 | 6.025 / 7.055 MiB | 2 / 1.002 MiB | 1.003 / 1.005 MiB |
| RocksDB | 64/10% | 12,075 | 19,753 | 9,913 | 17.9% | 10,186 | 21.007 / 21.016 MiB | 2 / 7.003 MiB | 7.003 / 7.019 MiB |
| SlateDB | 64/10% | 11,617 | 18,879 | 4,982 | 57.1% | 4,985 | 42.034 / 49.068 MiB | 2 / 7.003 MiB | 7.003 / 7.005 MiB |
| RocksDB | 512/1% | 62,278 | 118,923 | 8,700 | 86.0% | 9,127 | 18.041 / 18.018 MiB | 3 / 6.003 MiB | 6.003 / 6.016 MiB |
| SlateDB | 512/1% | 61,464 | 116,724 | 3,847 | 93.7% | 3,849 | 36.067 / 42.078 MiB | 3 / 6.003 MiB | 6.003 / 6.006 MiB |
| RocksDB | 512/10% | 95,053 | 154,917 | 77,751 | 18.2% | 78,378 | 156.052 / 156.056 MiB | 9 / 52.006 MiB | 52.008 / 52.024 MiB |
| SlateDB | 512/10% | 96,360 | 156,166 | 49,728 | 48.4% | 49,757 | 312.172 / 364.272 MiB | 9 / 52.006 MiB | 52.008 / 52.014 MiB |

SlateDB allocation is about 16.7% above current because authenticating old
immutable leaves adds read-owned buffers. Slate settled physical reads are also
about 50% above current in repeated samples (for example 163.601 MiB versus
109.056 MiB at 512/10%). The model still reduces wall and CPU substantially,
but the production implementation must preserve zero-copy/sliced read buffers
and re-qualify this adapter cost. RocksDB allocations are neutral after the
one-copy path-copy primitive.

## SlateDB resource-amplification follow-up

Status: **retain the accepted 1 MiB/F64/Q8 tree; reject smaller leaves and new
packing; implement exact-extent ownership transfer in the storage read path.**
This follow-up starts at model head
`4c7b1a5ccc97fc1a0466355ca84ad0717862c226` and remains benchmark-only.

Phase counters on the exact 64 MiB/1% Slate cell attribute the steady
7,398,239 allocated bytes as follows:

| Phase | Allocated bytes | CPU (typical) | Meaning |
|---|---:|---:|---|
| Root fetch/decode | 15,072 | 11–27 us | Small authenticated path metadata |
| One old-leaf fetch | 2,104,458 | 85–122 us | Object-store range plus a second 1 MiB reconstruction buffer |
| Authenticate/copy/hash successor leaf | 1,048,593 | 249–254 us | Required single mutable successor buffer |
| Atomic object/selector publication | 4,225,604 | 326–342 us | Existing Slate write ownership |

`ImmutableValueStore::get_many` already obtains each remote extent as `Bytes`,
but then reconstructs every requested value into a new
`BytesMut::with_capacity(requested.len())`. For this cell the requested value
is one complete 1,048,593-byte encoded leaf in one extent. Transferring or
slicing that exact owned span removes precisely 1,048,593 bytes. Projected
blocked allocation becomes 6,349,646 bytes, only 31,890 bytes (+0.50%) above
current's 6,317,756. Across the accepted four cells, subtracting exactly one
encoded old-leaf buffer per touched leaf leaves approximately +0.50%, +0.08%,
+0.03%, and +0.03% versus current. This is the allocation perfect-elimination
ceiling; successor bytes and write ownership remain required.

Physical reads have a different bound. A trusted root names immutable object
bytes, not a presence bit or cache entry, so every touched old leaf must be read
and hashed before its child ID can be reused. The lower bound is
`sum(encoded touched leaves) + touched path nodes`; for 64/1 it is 1,051,175
logical bytes. Slate's steady 3,155,111 physical bytes are its approximately
2.10 MiB current publication/settle work plus that mandatory old-leaf/path
read. Eliminating the latter would require another attestation or cache
authority and is rejected.

### Parameter and packing experiments

A fixed 64 KiB/F128 tree with a constant 8 MiB leaf batch improved the focused
Slate 64/1 cell versus the accepted 1 MiB tree: paired median wall/CPU -12%
(-20% in the initial focused profile), allocated bytes -30.2%, physical reads
-30.8%, and logical writes -30.8%. Against current, its
5,163,243 allocated bytes were -18.3%, and 2,181,855 physical read bytes were
only +3.9%. Rocks showed the same direction and exact cold reopen passed.

The cut fails at scale. At Slate 512/10, 820 small leaves produce 298,648
allocation calls; median wall rises from the accepted 49.7 ms to 69.7 ms
(+40%), CPU from 49.8 ms to 78.7 ms (+58%), and allocation from 364.3 MiB to
413.5 MiB (+13.5%). Reducing the key batch from 128 to 32 leaves changes neither
bytes nor allocation and changes wall by only about 1%, proving request count
is not the causal term. The fixed small-leaf layout is therefore rejected; no
adaptive second format is allowed.

Content-independent extent packing was also modeled. Authenticated objects and
their IDs remained the sole authority; the 60-byte
`ObjectId -> (extent, offset, length)` locator was treated as rebuildable
routing data. Packing base objects into approximately 63 MiB extents reduced
512/10 steady physical read objects from 37 to 13 and seed disk by 186,125
bytes (0.035%), but physical read bytes stayed 161,417,382 and allocation stayed
about 413.4 MiB. Median wall rose from 69.7 to 73.2 ms (+5.0%) and CPU from
78.7 to 84.7 ms (+7.6%). Existing range coalescing had already removed the byte
cost, so a new pack/locator layer is rejected. A 60-byte locator per 64 KiB
object is 61,440 bytes per 64 MiB before LSM key overhead; rebuilding it must
scan/frame/hash every packed object, `O(N)`, and GC must still trace object IDs.

The final complexity remains `O(KL + P*F*D)` CPU/hash work and
`O(KL + P*F)` authenticated read bytes with `L=1 MiB`, `F=64`, and `Q=8`.
Exact-extent ownership transfer changes the allocation coefficient, not Big-O.
The physical-read amplification is accepted as the authentication cost for the
single-authority layout.

### Additional implementer requirements for Ryzen-V

1. Keep the accepted fixed 1 MiB leaf, fanout 64, and eight-leaf/8 MiB bounded
   authentication batch. Do not add adaptive leaves or a second format.
2. Add a read-only storage primitive that can transfer or slice an already
   owned exact immutable extent into the returned `Bytes`. Use reconstruction
   allocation only when a value is genuinely fragmented. This is buffer
   ownership, never cache or content authority.
3. Authenticate the complete returned object bytes against the parent
   `ObjectId` before decoding or path copying. Zero-copy must not skip hashing,
   length checks, or fail-closed behavior.
4. Submit one coherent `get_many` per path level and retain the Q=8 memory
   bound. More or smaller calls did not reduce allocation.
5. Reuse the adapter's existing physical immutable segments only. If a routing
   locator can be rebuilt by framing and hashing segment bytes, it may remain a
   disposable index; it must not publish independently, name content, or gate
   GC. Do not add a new extent layout for this optimization.
6. Qualify exact-span and fragmented-span reads, malformed frame/range/locator,
   cold reopen, shared/final-reference GC, and the four 64/512 MiB 1%/10%
   adapter cells. Track allocation bytes and calls separately.

Logical writes and first-settled disk are within 1% of current because edited
payload dominates. Metadata improves slightly at 512 MiB: blocked writes
54,534,143 bytes at 10% versus current 54,549,897. Process peak RSS includes
the excluded 64/512 MiB seed. Final 512/10% process high-water marks were
858.3 MiB blocked versus 851.7 MiB current on RocksDB (+0.8%), and 865.0 MiB
versus 832.2 MiB on SlateDB (+3.9%).

All timed cells completed far below 20 minutes. Every final cell cold-reopened
and reconstructed exact successor bytes.

## Correctness result

The dual-adapter model proves:

- every loaded object key is recomputed from complete canonical bytes before decode;
- malformed domain, length, fanout, child ordering/length sum, missing object,
  or content mismatch fails the operation;
- the successor root equals a clean full canonical recomputation;
- immutable objects and selector move publish in one storage transaction;
- the selector move has an exact old-root CAS precondition;
- stale publication is rejected by both adapters (at begin or commit);
- cold reopen traverses and authenticates exact bytes;
- a leaf shared by two roots survives release of the first root;
- authenticated mark/sweep reclaims it only after the final root is released;
- corruption aborts traversal; no partial selector publication occurs.

An edit authenticates the selected root, every touched internal path, and every
touched old leaf. It cannot detect arbitrary physical corruption in an
untouched leaf without restoring `O(N)` work. Instead, the immutable parent ID
remains the sole expected identity, and any later range/full read, checkpoint,
or GC traversal that reaches the bad object fails closed. GC must abort the
entire sweep if any reachable object fails authentication.

## Ryzen-V implementation contract

1. **One immutable object authority.** Use the accepted ForkTree object space.
   Delete multimedia payload presence markers and the flat
   `BlobManifestV1 { ordered_chunks, content_digest }` authority. Do not add a
   side index, cache authority, delta chain, compatibility reader, or dual
   writer.
2. **Canonical leaf.** A leaf is domain tag, version, declared logical length,
   and payload bytes. Length is `1 MiB` except the unique final leaf. Its object
   key is the accepted keyed hash of all encoded bytes. Missing or mismatched
   key/value bytes are corruption, never ineligibility.
3. **Canonical internal node.** Encode version, level, subtree logical bytes,
   child count, and ordered `(child ObjectId, child logical bytes)` entries.
   Fanout is 64. Reject zero/excess fanout, wrong level, noncanonical partial
   nodes, overflow, or a child-length sum different from the subtree length.
4. **Single blob identity.** The root object ID is the blob identity (a type
   wrapper/domain projection is allowed; a second digest is not). The exact
   same ID must be used by transaction-local payload ownership, file content,
   persisted blob-ref row, checkpoints, reads, and GC roots.
5. **Path discovery.** Resolve byte offsets using authenticated subtree lengths.
   Authenticate root and touched internal nodes before trusting child IDs or
   bounds. Batch internal reads per level.
6. **Bounded leaf authentication.** Read touched old leaves in fixed batches of
   at most `Q=8` (8 MiB). Authenticate encoded bytes in place. Make exactly one
   mutable copy of each old leaf object, splice into its payload region, hash
   the complete successor leaf, and retain the resulting immutable bytes for
   staging. Do not decode-copy, edit-copy, then encode-copy.
7. **Path copy.** Reuse only authenticated untouched child IDs. Re-encode and
   hash every touched ancestor bottom-up. Derive the successor solely from the
   complete new root. Work is proportional to touched leaves/paths, never `N`.
8. **Atomic publication.** Stage all new leaves/internal nodes and the exact
   visible blob-ref/semantic row in one existing `StorageWriteSet`. Move the
   existing root selector and rotate the existing global publication epoch in
   that same commit with exact old-selector/epoch preconditions. No CAS
   precommit, partial object authority, or second flush boundary.
9. **Fail closed.** A named base with any missing, malformed, wrong-domain,
   wrong-length, reordered, or hash-mismatched reached object returns
   corruption before staging a selector move. Never fall back from a corrupt
   named base. With the hard cut, old noncanonical/flat/delta layouts are not a
   compatibility path.
10. **Reads/checkpoints.** Point/range reads authenticate each traversed node and
    leaf. Checkpoints retain the root ID, not a re-materialized digest or flat
    manifest. Reopen uses the same reader.
11. **Reclamation.** Existing root projection is the only root universe. Mark
    recursively through authenticated child IDs with bounded pages. Shared
    objects survive until every branch/checkpoint/history root releases them.
    Any reachable corruption aborts sweep. Existing epoch/CAS fences prevent a
    concurrent publication from losing a child.
12. **Qualification.** Preserve these 64/512 MiB, 1%/10%, Q=8 cells and add
    noncontiguous edits, final partial leaf, branch/diff/merge/checkpoint/reopen,
    every node/leaf corruption class, selector race, and shared/final-release
    GC. Require both adapters to retain >10% wall/CPU improvement and no
    unaccepted critical >5% regression. In particular, remove or explicitly
    accept SlateDB's measured ~16.7% allocation and ~50% physical-read increase.

## Design convergence and borrowed lessons

- Dolt's Prolly Trees use copy-on-write content-addressed Merkle nodes; large
  values become leaf chunks under a root address. This supports root identity
  and path copying, but multimedia should use deterministic fixed-size leaves
  rather than content-defined relational boundaries.
- IPFS UnixFS provides the closest file analogue: fixed-size raw leaves under a
  balanced, bounded-width content-addressed DAG, with subtree sizes for seek.
- LMDB demonstrates the small mutable root plus copy-on-write B+tree path and
  serialized-writer model, but lacks authenticated content IDs.
- Neon separates immutable historic layer files from mutable layer maps and
  retains ancestor layers for branches/GC. That reinforces immutable payload
  ownership and explicit root retention, not presence markers.
- Turso splits files into 128 KiB segments grouped into generations; branches
  share segments and diverge by metadata. It reinforces metadata-only branch
  roots and on-demand immutable segment reads.
- DuckDB uses fixed blocks/row groups plus explicit checkpoint compaction and
  storage versions. It reinforces bounded physical units and separate
  compaction, though not a Merkle identity design.

Primary sources:

- Dolt Prolly node/chunker source: <https://github.com/dolthub/dolt/tree/main/go/store/prolly/tree>
- Dolt large-value Merkle description: <https://www.dolthub.com/blog/2024-07-15-json-prolly-trees/>
- IPFS balanced UnixFS DAGs: <https://docs.ipfs.tech/concepts/file-systems/>
- LMDB copy-on-write B+tree overview: <https://www.symas.com/post/is-lmdb-a-leveldb-killer>
- Neon immutable layers/branch retention/GC: <https://github.com/neondatabase/neon/blob/main/docs/pageserver-storage.md>
- Turso segments/generations/branching: <https://turso.tech/blog/how-does-the-turso-cloud-keep-your-data-durable-and-safe>
- DuckDB blocks/row groups/storage versions: <https://duckdb.org/docs/stable/internals/storage>
