# Repository sync

Lix synchronizes its native repository facts rather than maintaining a second
row protocol:

- complete immutable commits, with merge provenance in the second parent and
  self-contained checkpoint members;
- compare-and-swap branch ref updates on one ordered repository cursor;
- BLAKE3/FastCDC blob manifests and chunks.

`repository.rs` exports, imports, and persists those facts. `runtime.rs` is the
one bootstrap, outbox, long-poll, reconciliation, and retry state machine.
`contract.rs` is its transport interface. `platform.rs` and `platform/` contain
the only native/browser divergence: tasks, timers, HTTP, and cancellation.

An initial pull pins the cursor, default branch, and branch heads. The runtime
then fetches distinct head commit bodies with bounded topology certificates and
immutable head-pinned current-row pages concurrently. Live events transfer
complete commits and ref moves. Older commit bodies and binary chunks load
separately on demand and never advance the live cursor.

Checkpoint inventory pages carry canonical headers without hydrating historical
state. Bootstrap records which headers came only from that inventory; only
those deferred checkpoints may omit jump boundaries. Serving-history headers
and normal history imports still require their jump closure. A missing deferred
graph node triggers ordinary bounded history demand before traversal continues.
Known jump generations, self-jumps, invalid spans, and inventory/body identity
mismatches are validated before publication.

Sync-mode format upgrades retain the previous source and bootstrap a new physical
generation from the authenticated authority. The source is never reused as a
later candidate. A durable manifest records source identity and recovery status
before activation; failed bootstrap leaves the source active and retryable.
Ordinary writes are fenced by the exact active pointer, and retained-source
readers cannot write. Standalone and authority repositories use format migrations.

Admission checks identity and classifies local work, without requiring a clean
replica. Acknowledged branch/head/checkpoint coordinates and redundant engine
bookkeeping do not block opening. Pending edits, incomplete uploads and custom
local-only rows remain preserved. Unknown identity metadata fails without altering
the source; an arbitrary server URL is not sufficient proof of identity.

`replica_recovery_sources`, `export_replica_recovery`, and `recover_replica` expose
retained work after opening. Export reads logical serving rows, original branch
coordinates, available commit/blob data and unresolved dependencies. Restoration
creates independent branches preserving row/file IDs, with a transactionally
recorded, tracked provenance receipt that survives synchronization and later
rebuilds. Local-only rows stay local. A restoration
receipt neither authorizes source deletion nor proves server acceptance.

Generation retention and pointer publication are metadata-only, with no source
copy or history traversal. Classification scans branch controls and current rows;
bootstrap costs current server state plus ordinary checkpoint inventory. Recovery
runs separately and can require reading substantial retained data. Source listing
scales with the number of retained generations. Storage-space exhaustion fails
safely instead of recycling retained work.

In particular, retention/activation metadata work is O(1), and listing sources is
O(G) for G generations. Classification visits branch controls, current serving
rows and upload metadata; it does not traverse historical commits. Explicit
recovery also visits available pending history and payload bytes. Blob/file
lookups use maps rather than repeatedly scanning all files for every row. Path
reconstruction additionally pays for the directory components in the exported
paths. Normal transaction validation and index maintenance still apply when
publishing recovered rows. Resolving the fixed recovery baseline follows the
global first-parent graph using available jump links: O(H) reads in the worst
case per recovered branch for H historical ancestors without jump metadata,
with O(1) cursor
state. Each cold graph node uses normal authenticated sync-demand retries while
retaining the cursor, so hydration does not rescan the previously visited path.

A partial replica with on-demand sync keeps local commits while authority changes
arrive. Background reconciliation uses the same native row application pipeline
as branch merging, including registered schema/plugin merge hooks and file
serialization from resolved rows. For overlapping values without a custom merge,
the incoming operation wins when the server durably accepts it. Client clocks
and change IDs do not determine precedence. Opaque file content is atomic.

A partial SQL read can also prepare registered schema metadata needed for later
local writes. This uses the existing catalog validation path without loading rows
from those schemas. Built-in schemas use their embedded definitions; their stored
projections do not define the engine catalog. Repository opening does not perform
this preparation.

`POST /sync/retained-bodies` atomically pins each complete accepted native wave;
`POST /sync/merge` applies the frozen `B -> L` intent against the current selected
head `R` observed inside the authority transaction and publishes `[R,L]` plus its
immutable attempt receipt under that transaction's control guards. The request's
captured remote head remains identity and ancestry evidence, not a compare-and-swap
promise spanning network requests. Already included intent publishes an empty native
acknowledgment through the same owner without reapplying rows; identical parents are
deduplicated. Once captured L is already included, its checkpoint intent is not
replayed over the current authority checkpoint either. A submitted GLOBAL coordinate
remains an immutable dependency, while the authority may advance its catalog and
validate incoming rows against that current catalog. Ordinary concurrent row edits
resolve automatically. Retrying an accepted attempt returns its original receipt
and cannot reorder that write. A newer local `L2` remains pending until client
candidate adoption proves its exact serving basis; receiving a merge receipt
alone never advances ordinary upload confirmation.

Checkpoint compaction can remove working commits from causal parent history while
preserving their complete state. Incorporation proofs therefore follow authenticated
complete-state sources as well as causal parents; selecting only a subset does not
prove incorporation of its source. Checkpoint lineage itself remains a causal proof.
Format 80 records this provenance independently of selected mutation membership.
Checkpoint certification compares complete native value identities and row
lifetimes. It permits only the checkpoint planner's Added-row creation-time rebase,
verified against absence at the original checkpoint base. Rootless endpoints use
their native first-parent replay intervals; rooted endpoints retain Merkle pruning.
Absent rows and tombstones are equivalent for this logical-state proof, just as
they are in native checkpoint diff; historical mutation membership is unchanged.
This certification applies to new publication and import, not legacy migration.
Every legacy header migrates to unknown incorporation regardless of resident
evidence; causal ancestry and existing complete-state aliases remain separate
positive witnesses. Unknown provenance must not authorize replay as though the
incoming edit had never been included. Migration preserves resident inputs and
pending coordinates, including for sparse replicas.

Certified snapshots can name semantic source commits whose graph and body were
not transported. The mutable `omitted-local`, `omitted-global`, and
`omitted-unknown` markers distinguish this omission from deferred bodies with
known graph headers. No graph is fabricated, and unknown author scope is never
guessed from the containing branch. The v79-to-v80 migration repairs missing
markers only from durable certified replica coordinates and independently
materialized complete-state roots, under shared entry/byte limits and the same
atomic publication as the format marker. Arbitrary HOT references grant no such
exemption. Immutable legacy incorporation remains uniformly `LegacyUnknown`.

Graph work is resumable across missing-input fetches, and its work budget schedules
slices rather than rejecting a replica based on history age. Negative incorporation
queries stop at a proven confirmed base to avoid revisiting older history.

Long local histories publish through bounded oldest prefixes, preserving later
edits and advancing checkpoint intent only with its original working continuation.
The GLOBAL ordinary upload lane can progress while a selected merge is pending.
Selected prefixes stop at unpublished catalog dependencies, so a branch created
from pending local work can publish after its source is accepted, before later
selected edits that depend on the new catalog.

Merge requests distinguish the original confirmed checkpoint, captured authority
checkpoint, and captured local checkpoint. At acceptance, an unchanged local
checkpoint preserves the current authority checkpoint; an explicit local checkpoint
change retains its incoming precedence. Equal checkpoint overrides are omitted
canonically, preserving existing immutable request digests. The owned persisted
journal migration upgrades older records without resetting pending edits. These
endpoints require sync protocol 14/server protocol 8; upgrade SDK and server together.

Partial replicas deliver retained working-set updates through `POST /sync/update`.
The request contains native read recipes, and the response pairs a leased descriptor
with bounded immutable inputs selected by the same candidate evaluator used locally.
The server does not publish client coverage: the replica installs validated bytes,
then runs its existing candidate preparation and atomic publication. Idle polls carry
no working-set payload. Bundles exceeding the bounded delivery budget use existing
on-demand hydration. Initial opening still requests only bounded metadata.

Small-file publication includes canonical content in the existing `inlineBlobs` push
(up to 256 KiB per blob and a 1 MiB combined request); larger uploads retain the
existing chunk path. Authoritative row/plugin merge semantics are unchanged.
The new update route requires SDK and server to upgrade together. Repository storage
format is unchanged; existing partial replicas retain their data and read interests.
