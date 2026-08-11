# Does `untracked` need a second plane?

Design analysis of the untracked state lane in lix: why it exists, where it
physically lives today, what the status quo costs (measured), and a
recommendation across three candidate designs.

- Machine: `ryzen-9950x-IV` (32 cores, 186 GB), warm build.
- Tree: `claude-3-optimizations` @ `43de7b54d`, worktrees `~/claude3/base` and `~/claude3/cand`.
- Branch: `exp/untracked-analysis`.

---

## 0. Answer first

**Recommendation: design C — remove the untracked lane. Rebase PR #1273 onto
`claude-3-optimizations` and land it.**

The measurements below are not close. On the same 10,000-row fixture:

| | status quo, tracked | status quo, untracked | ratio |
|---|---|---|---|
| one single-row update | **280 µs** | **11,131 µs** | **39.7× slower** |
| settled physical bytes after 21 mutations | **313 KB** | **42,471 KB** | **135× larger** |
| full entity scan, one untracked row present | 3.755 ms | 17.493 ms | **4.66× slower** |

The lane that exists to be the *cheap* path for fast-mutating state is, today,
39× slower per mutation and 135× larger on disk than the full version-controlled
path it was meant to avoid — and it leaks, because the sweep that would reclaim
its stale generations is `#[cfg(test)]`.

Design B (the owner's hypothesis) is worth taking seriously and is evaluated
honestly in §5. The conclusion there is that **B is not a third design**: a row
that carries a real `change_id` and `commit_id` and lives in the one tracked
plane *is a tracked row*. B is C plus a boolean column. Whether to keep that
column is a separate, much smaller question — and the answer is "not in the
engine" (§5.3).

---

## 1. Why untracked exists — the history

### 1.1 It began as a key-naming hack for UI state

The oldest ancestor is not `untracked` at all. From the vendored SDK changelog
(`2b8db3c49:packages/sdk/CHANGELOG.md:285-298`, released as `0.1.0`):

> `b74e982: refactor: replace `#*` key syntax with `skip_change_control` column`
> …
> `Enables matching of flags and avoids re-names of keys. This change replaces
> the `#*` key syntax with a `skip_change_control` column in the `flags` table.`

```diff
 key_value = {
-  key: "#mock_key",
+  key: "mock_key",
+  skip_change_control: true,
   value: "mock_value",
 }
```

So version 1 of the concept was **a marker on a row**, not a plane. Nothing in
this repo's history documents a decision to make it a plane; the rename to
`lixcol_untracked` has **no changelog entry at all**.

The clearest statement of the original motivation survives in a *consumer* diff,
`1878f29b8 2025-07-07 Enhance useKeyValue hook to support global and untracked
options across components`, which replaces the old comment verbatim:

```diff
 		await trx
-			.updateTable("key_value")
+			.updateTable("key_value_all")
 			.set({
 				value,
-				// skip change control as this is only UI state that
-				// should be persisted but not controlled
-				// skip_change_control: true,
+				lixcol_untracked: options.untracked,
 			})
```

> *"this is only UI state that should be persisted but not controlled"*

The docs of the era say the same thing and nothing more
(`2b8db3c49:packages/docs/docs/guide/concepts/key-value.md`):

> `- **Store UI state as untracked**: Use `lixcol_untracked: 1` for ephemeral data`

and `2b8db3c49:packages/sdk/src/entity-views/README.md:129`:

> `- `lixcol_untracked` - Bypasses change tracking (for UI state)`

**The problem untracked was introduced to solve: "sidebar width / dismissed
prompt / active file" rows should persist but should not appear in history or
diffs.** That is a *presentation* problem, not a storage problem.

### 1.2 It became a plane because the SQLite engine had no other way to express it

RFC 001 (`/Users/samuel/git-repos/lix/rfcs/001-preprocess-writes/index.md`,
dated 2025-11-24) is where the plane is written down. Untracked is lane 3 of 4:

> `3. **Untracked state** – `lix_internal_state_all_untracked``
> `   - Local-only changes; not synced; coexist with transaction/committed rows.`

```
        ┌───────────────┐ ┌───────────┐ ┌─────────────────────────────┐
        │  Transaction  │ │ Untracked │ │      Committed State        │
        │    State      │ │   State   │ │      (cache tables)         │
        │   (staging)   │ │  (local)  │ │                             │
        └───────────────┘ └───────────┘ └──────────────▲──────────────┘
...
                        Prioritized UNION
                      (transaction > untracked > committed)
```

and the read path is defined *as* a union over the three tables (line 93):

> `The preprocessor intercepts SELECT queries and rewrites them into a `UNION`
> query combining the three physical tables, using `ROW_NUMBER()` to prioritize
> uncommitted/untracked changes.`

This is the origin of "reads must fetch both". **The reason it is a plane is
that the 2025 engine was a SQL preprocessor over SQLite tables.** The only way
to say "these rows are not history" was to put them in a different table, and
then the only way to read them was a `UNION ALL` branch with a `ROW_NUMBER()`
precedence rule. The design is an artifact of that implementation, not of the
requirement.

Note what the RFC does *not* say: its "Benefits" section proposes eliminating
`lix_internal_transaction_state` ("we no longer need a separate table to stage
uncommitted changes") but never questions the untracked lane.

### 1.3 The Rust rewrite copied the plane, then spent a year unwinding it

`ac2fda11b 2026-02-03 untracked state` reimplements the lane in Rust, keeping
the SQL-rewrite routing (the first test is literally
`untracked_state_routes_to_untracked_table`). `019cf7955 2026-02-04`:

> `- Added functionality to rewrite INSERT statements targeting the vtable to
>    route untracked entries to a separate untracked table.`

Then untracked stopped being "UI state" and became **engine infrastructure** —
`00f0fef33 2026-03-17 Make version refs untracked` — which is why it is now
entangled with branch lifecycle.

The cost showed up immediately. `be8709d56 2026-03-24 Consolidate engine modules
under live_state/`:

> `Extract shared SQL utilities, unified request types, and row identity logic
> to eliminate **~1200 lines of duplication between tracked and untracked state
> modules**.`

Then a dedicated storage space (`0x0001_0002 untracked_state.row.v1`), a
dedicated optimization log (`722a09d25:optimization_log10_untracked_state.md`):

> `Goal: make `packages/engine/src/untracked_state` fast for durable local
> overlay CRUD without moving the workload through tracked-state, changelog, or
> SQL2.`

and roughly **25 optimization commits** in May 2026 alone (`738f7221e`,
`c7121aa91`, `b5a99e078 "Make untracked state a pure sidecar"`, `9d3dcb1b4`,
`ac0ab7155`, … `781dd6a35 "Optimize untracked state physical layout"`, merged as
PR #445 "Untracked speedup"). That entire campaign existed only to make the
second plane keep up with the first.

Then the unwinding began:

| commit | date | what it removed |
|---|---|---|
| `4af391802` | 2026-07-12 | `Unify tracked and untracked change ledger` — deletes `untracked_state/{codec,context,materialization,storage,types}.rs` (1,392 lines) |
| `df79f3137` | 2026-07-26 | `perf: unify current tracked and untracked state` — deletes `live_state/index/*` (~1,530 lines); untracked stops having its own index |
| `d2fcffef7`, `bde63dc1f` | 2026-08-04 | `isolate untracked state physically` / `isolate untracked row authority` |
| `48acfccc4` | 2026-08-11 | space `0x0001_0002` formally listed as **retired** |

The surviving comment from `df79f3137` states the intent
(`live_state/tracked_head.rs:326-331`):

> `/// This is deliberately the single write representation for the hot state
> /// plane, so callers never stage a separate untracked overlay.`

**So the project has already decided, twice, that untracked should not be a
separate physical lane. What remains is the residue of that decision.**

### 1.4 What the original design assumed that is no longer true

Four assumptions, all dead:

1. **"Diffs are a fixed view, so exclusion must be physical."** False since
   `b42c7b025 2026-07-29 feat(engine): add SQL diff commands` and
   `862a9ecce finalize SQL diff command contract`. `docs/diff-commands.md`:
   > `Lix represents a diff as rows. … The source is a normal SQL query, so
   > filters, joins, ordering, and limits stay`
   A diff is now a relation you can filter. Exclusion is a `WHERE` clause.
2. **"Reads are a `UNION ALL` over physical tables, so a marker column cannot
   route."** False — the engine is no longer a SQL preprocessor. `lixcol_untracked`
   is already a *public, readable* column
   (`sql2/catalog/registry.rs:597`, `:628`: `PublicColumn::public_insert_only("lixcol_untracked", false)`
   — `public_insert_only` means *readable, not updatable*, `sql2/catalog/schema.rs:30-43`).
   `WHERE lixcol_untracked = FALSE` already works, and already routes
   (`live_state/reader.rs:11-15,60-65`).
3. **"Untracked is UI state: few rows, cheap."** False in practice: branch refs,
   the workspace branch selector, deterministic-mode engine rows and plugin
   state all live in this lane now (`branch/stage_rows.rs:53,84`,
   `session/switch_branch.rs:136`, `session/context.rs:64`).
4. **"The second plane is faster because it skips history."** Measured false —
   §4. It is 39× slower per mutation and 135× larger on disk.

### 1.5 The problem itself still exists — in a much weaker form

The owner's two surviving requirements are real:

- *"don't show me rows that are untracked"* — real, and already satisfied by the
  public marker column plus SQL diff surfaces.
- *"a checkpoint could checkpoint only certain rows"; history GC for
  fast-mutating state* — real, and this is a **retention policy** question, not
  a storage-lane question. Nothing about it requires a second plane, a second
  generation root, or NULL `commit_id`s.

---

## 2. What the second plane is, physically, today

### 2.1 The dedicated space is already gone — confirmed

`packages/lix/src/storage_spaces.rs:78-84`:

```rust
#[cfg(test)]
pub(crate) const RETIRED_STORAGE_SPACE_IDS: &[StorageSpaceId] = &[
    // untracked_state.row.v1
    StorageSpaceId(0x0001_0002),
    // live_state.index.branch_root.v1
    StorageSpaceId(0x0004_0005),
];
```

`grep -rn "0x0001_" packages/lix/src` returns exactly two lines: this one and the
`#[cfg(test)]` predecessor-bytes fixture at `engine.rs:562-570`
(`engine_ignores_predecessor_state_bytes_and_leaves_them_untouched`). **There is
no production writer or reader of the untracked space.** The prior audit was
right. Of the 46 registered spaces, only one is untracked-specific and it is a
GC side-table: `0x0002_0002 json_store.untracked_reclaim_candidate.v1`
(`json_store/context.rs:26`).

### 2.2 The second plane is now a second *generation root*, not a second space

Untracked rows live in `HOT_ROW_SPACE` (`0x0004_001b live_state.hot_row.v21`),
the same space as tracked rows. The physical key is:

```
[4B space][branch_id \0][16B generation uuid][schema_key \0][file_id][entity_pk]
```
(`hot.rs:10513`, `10528-10540`; `encode_scope_prefix` at `tracked_head.rs:913-918`)

Untracked-ness is **not** in the key. What separates the lanes is which
generation UUID the branch control names — and the branch control names **two**:

```rust
pub(crate) struct BranchHeadControl {
    pub(crate) head_commit_id: CommitId,
    pub(crate) tracked_generation: CommitId,
    pub(crate) untracked_generation: CommitId,
    ...
```
(`branch/control.rs:44-49`)

The untracked generation is already a `CommitId`-shaped value — a blake3 of
`(branch_id, previous_generation, revision)`
(`branch/control.rs:152-166`) — it is simply not a commit that exists in the
changelog. **The second plane today is one extra 16-byte pointer in the branch
control, and everything that follows from having two serving roots instead of
one.**

The two roots start equal at branch birth and diverge on the first untracked
write. Nothing ever sets them equal again: `untracked_generation` is only ever
advanced by `untracked_lifecycle_generation` (`commit.rs:3767, 4424, 4472, 4555,
5104`) and `tracked_generation` only by the tracked publication
(`normal_branch_head_control`, `commit.rs:4964-4967`, which *inherits*
`untracked_generation` unchanged). **The split is permanent for the life of the
branch.**

### 2.3 The marker itself is one bit and costs nothing

`live_state/tracked_head.rs:1350-1356`:

```rust
const HEAD_VALUE_VERSION: u8 = 8;
const HEAD_VALUE_HEADER_BYTES: usize = 59;
const HEAD_VALUE_DELETED: u8 = 0b0000_0001;
const HEAD_VALUE_UNTRACKED: u8 = 0b0010_0000;
```

It is a spare bit in an existing flags byte (`tracked_head.rs:1693-1697`,
decoded at `:1888`). **Zero extra bytes per row.** In memory the materialized
batch keeps a `Vec<bool>` column, 1 byte/row (`live_state/types.rs:116,386`).

The encoder does pay a 4-way exhaustive match plus three guards on every row
(`tracked_head.rs:1629-1660`), but that is a handful of predictable branches.

**Conclusion: the marker is free. The plane is not.** These are separable, and
that separation is the whole design question.

### 2.4 What "reads fetch both" actually is

| where | file:line | shape |
|---|---|---|
| scan | `hot.rs:4525-4586` | two full generation scans, each filtered by the bit, concatenated |
| exact/point | `hot.rs:5380-5443` | two point-read batches, per-index `or_else` fallback |
| uniqueness validation | `transaction/validation.rs:298-317` | two `load_exact_batch` probes, `extend`ed |
| FK/domain resolution | `domain.rs:117-155` | `untracked: bool` is part of the domain key |
| filesystem plan cache | `filesystem/planner.rs:1574-1580` | cache key includes `untracked=`, doubling the cache |

The scan code names the problem out loud (`hot.rs:4563-4571`):

> `// A split branch control has two independent serving roots: the tracked
> // selector owns only tracked rows and the untracked selector owns only
> // current-only rows. Older generations can contain a complete pre-split
> // snapshot, so concatenating both roots without applying the domain boundary
> // would resurrect tracked rows from the untracked root after checkout/merge.`

and the exact path calls them (`hot.rs:5426`) *"two independent authority
domains"*. **That is layout invariant #1 (single authority) failing, in a
comment, in the hot path.**

### 2.5 The expensive part is not the second scan — it is the fast paths that turn off

Three tracked-plane optimizations are disabled outright when untracked is in
play:

```rust
// live_state/context.rs:549 — columnar entity layout
if ... || request.filter.untracked.is_some() || ... { return Ok(None); }

// live_state/context.rs:635 — direct immutable-base snapshot scope
if request.filter.untracked.is_some() || request_may_include_derived(request) { return Ok(None); }

// live_state/context.rs:664-666
// The direct immutable-base projection covers one serving generation.
// A split selector must use the merged tracked/untracked visibility path ...
if requested_control.tracked_generation != requested_control.untracked_generation { return Ok(None); }

// live_state/context.rs:1443-1445 — schema-presence bloom
if request.filter.untracked.is_some() { return true; }   // i.e. give up, always scan
```

And two write fast paths refuse untracked deltas by construction:
packed collection replacement (`hot.rs:6244-6253`, `|| delta.untracked` → bail)
and packed current base (`hot.rs:6516-6524`, hard error).

So: **one untracked row anywhere in a branch permanently disables the columnar
projection, the immutable-base projection and the presence bloom for every
entity scan on that branch.** That is the mechanism behind the read numbers in
§3.

### 2.6 Every untracked write republishes the entire untracked population

`hot.rs:6807-6866`, `stage_untracked_generation`:

> `/// Publishes a new branch-local untracked snapshot without touching the
> /// tracked generation. The old untracked rows are copied as encoded values,
> /// then the sorted untracked deltas are applied once before the complete new
> /// generation is staged.`

```rust
let mut rows = load_hot_untracked_generation(self.store, branch_id, previous_generation).await?;
for delta in sorted { apply_complete_hot_snapshot_delta(&mut rows, delta, ...)?; }
...
stage_complete_hot_rows(self.writes, branch_id, new_generation, rows);
```

`load_hot_untracked_generation` (`hot.rs:8073-8104`) is an **unbounded full scan**
of the previous generation prefix; `stage_complete_hot_rows` rewrites **every**
surviving row under a fresh generation UUID. One untracked mutation costs O(total
untracked rows in the branch) reads *and* writes.

### 2.7 The stale generations are never collected in production

The sweep exists — `TrackedHeadContext::stage_collect_stale_current_state_generations`
(`live_state/tracked_head.rs:482`) — but it has exactly two callers, and both are
tests:

- `gc.rs:2728`, inside `#[cfg(test)] async fn stage_repository_gc_full_recovery`,
  documented at `gc.rs:2673-2676` as
  > `/// Recovery-only verifier retained for explicit rebuild tooling and tests.`
  > `/// Ordinary maintenance never calls this path`
- `live_state/tracked_head.rs:5159`, inside `mod tests` (opens at `:2255`).

Production `stage_repository_gc` / `stage_repository_gc_with_preconditions`
(`gc.rs:2254-2640`) contain **zero** occurrences of `untracked`, `stale`, or
`generation`. The same is true of the reclaim-candidate space `0x0002_0002`:
it is *written* by production (`hot.rs:6860, 7609, 7694`) and *drained* only by
the `#[cfg(test)]` path (`gc.rs:2757, 2783`).

**Consequence: superseded untracked generations accumulate forever.** §4.3
measures it.

---

## 3. Measured: read cost of the status quo

### 3.1 Method

Pure-fixture A/B on a **single binary** — no recompilation between arms, so
worktree/codegen noise is structurally excluded.

- Bench: `tracked_state_crud`, `sql_session` layer, `real_workload` (10k rows), RocksDB and SlateDB.
- Arms: `N1` = no untracked rows; `UU` = `LIX_TRACKED_STATE_CRUD_PROFILE_UNTRACKED=one_unrelated`
  (the fixture substitutes one of the 10,000 rows with an untracked probe, so
  total row count is unchanged); `N2` = **null control**, byte-identical to `N1`.
- **15 reps per arm, 45 arm-runs, arm order rotated** so each arm takes each
  position 5 times.
- Binary: `~/claude3/base/target/release/deps/tracked_state_crud-dfe69a0deced0469`
  (`claude-3-optimizations` @ `43de7b54d`).
- Command per arm:
  `./tracked_state_crud --bench 'sql_session/lix_(rocksdb|slatedb)/real_workload/(read_all_rows_consumed|read_all_rows/|read_one_by_pk|read_many_by_pk)'`

### 3.2 Results (median of 15, ms)

| bench | N1 | N2 (null) | UU | null control | UU vs none |
|---|---|---|---|---|---|
| rocksdb `read_all_rows/10k` | 2.5582 | 2.5621 | 16.0080 | **+0.15%** | **+525.5%** |
| rocksdb `read_all_rows_consumed/10k` | 3.7551 | 3.7560 | 17.4930 | **+0.02%** | **+365.8%** |
| rocksdb `read_one_by_pk/10k` | 1.2327 | 1.2350 | 1.1441 | +0.19% | −7.21% |
| rocksdb `read_many_by_pk/10` | 1.3271 | 1.3281 | 1.2436 | +0.08% | −6.33% |
| slatedb `read_all_rows/10k` | 2.6820 | 2.6840 | 17.1140 | +0.07% | **+537.6%** |
| slatedb `read_all_rows_consumed/10k` | 3.8791 | 3.8776 | 18.3400 | −0.04% | **+372.9%** |
| slatedb `read_one_by_pk/10k` | 1.2789 | 1.2757 | 1.2054 | −0.25% | −5.59% |
| slatedb `read_many_by_pk/10` | 1.3771 | 1.3786 | 1.3264 | +0.11% | −3.75% |

**The null control's spread is ±0.25%.** That is the resolution floor of this
harness in this configuration, and it is unusually tight because both arms are
the same binary. Every effect reported here is far outside it; the scan arms'
raw ranges do not overlap at all (rocksdb `read_all_rows_consumed`: N ∈
[3.690, 3.799], UU ∈ [16.997, 18.330]).

p95 (ms), rocksdb `read_all_rows_consumed`: N1 3.790 / N2 3.780 / UU 18.013.

Raw per-rep medians are in `/root/claude3/expY-readtax.log` on `ryzen-9950x-IV`;
the parser is `scratchpad/parse_readtax.py`. A representative row:

```
rocksdb read_all_rows_consumed N1: 3.690 3.761 3.739 3.799 3.759 3.749 3.727 3.751 3.769 3.755 3.763 3.732 3.738 3.790 3.769
rocksdb read_all_rows_consumed N2: 3.773 3.756 3.735 3.780 3.763 3.775 3.773 3.741 3.783 3.754 3.751 3.745 3.720 3.754 3.774
rocksdb read_all_rows_consumed UU: 17.976 17.485 17.170 17.930 17.639 18.013 17.184 18.330 17.324 17.140 17.495 17.923 17.493 17.267 16.997
```

### 3.3 Reading these numbers honestly

- **Scans: catastrophic.** One untracked row in a 10,000-row table costs
  **+366% to +538%** on full entity scans, on both backends. A dual scan alone
  could explain at most ~2×; 4.7–6.3× is the *fast paths turning off* (§2.5). The
  byte-level confirmation is in §4.3: the untracked fixture has **no**
  `entity.columnar_row_group_column.v1` and **no** `packed_current_base` rows at
  all, while the tracked fixture has 15 and 1.
- **Point reads: mildly faster (−3.8% to −7.2%), not slower.** This is real and
  outside the noise floor, and it cuts *against* my recommendation, so it is
  worth stating plainly. The likely mechanism is that the untracked publication
  forces a complete hot republish, flattening the tracked generation into plain
  hot rows and removing the packed/columnar indirection a point read would
  otherwise traverse. In other words the point read is faster **because** the
  scan optimizations were destroyed. It is not a benefit of the lane.
- **The dual-lane tax is zero when idle.** With no untracked rows the
  generations are equal and both read paths short-circuit
  (`hot.rs:4560`, `:5396`). The null-vs-N1 delta of 0.02–0.19% confirms there is
  no measurable always-on overhead. **The cost is entirely conditional on the
  lane actually being used.**

---

## 4. Measured: write cost and byte cost of the status quo

### 4.1 Method

Purpose-built probe committed to this branch:
`packages/engine-benchmarks/examples/expY_untracked_mutation_scaling.rs`.
It seeds `U` `lix_key_value` rows in one lane, warms once, then times 20
single-row `UPDATE`s through the real `SessionContext` SQL path on RocksDB, and
reports per-space `layout_accounting` at the end. The tracked lane is the control
at the same population on the same surface.

`cargo build -p lix_benchmarks --release --features storage-benches,slatedb --example expY_untracked_mutation_scaling`
then `./target/release/examples/expY_untracked_mutation_scaling 20 <populations>`
in `~/claude3/cand`.

### 4.2 One untracked mutation is O(population); one tracked mutation is O(1)

Median µs per single-row update, 3 independent runs:

| population | untracked (run 1 / 2 / 3) | tracked (run 1 / 2 / 3) | untracked ÷ tracked |
|---|---|---|---|
| 1 | 151.8 / 152.9 / 121.3 | 198.6 / 191.2 / 192.9 | 0.7× |
| 10 | 173.5 | 190.2 | 0.9× |
| 100 | 307.8 / 312.3 / 242.0 | 192.4 / 192.4 / 193.4 | 1.5× |
| 1,000 | 1,496.2 / 1,488.2 / 1,206.4 | 273.0 / 274.2 / 274.2 | 5.4× |
| 10,000 | **11,056 / 11,121 / 11,322** | **281.0 / 282.9 / 279.4** | **39.7×** |

Ratio vs population 1: untracked **72.8× / 72.8× / 93.4×**; tracked **1.41× /
1.48× / 1.45×**. Mean and median agree to within 0.5% on every cell, and the
arms' ranges are separated by two orders of magnitude, so 3 runs is sufficient
under the runbook's exception for non-overlapping ranges.

**The untracked curve is linear in population; the tracked curve is flat.** This
is `stage_untracked_generation` (§2.6) doing exactly what its doc comment says.

### 4.3 Physical bytes: the "no history" lane is 135× larger than history

Same fixture, population 10,000, after 21 single-row updates, per-space
`layout_accounting`:

| space | untracked lane | tracked lane |
|---|---|---|
| `0x0004001b live_state.hot_row.v21` | **220,040 rows / 42,457,577 B** | 41 rows / 22,269 B |
| `0x0004001a tracked_state.commit_delta_segment.v6` | — | 20 rows / 137,818 B |
| `0x0004002a entity.columnar_row_group_column.v1` | — | 15 rows / 110,820 B |
| `0x00040024 live_state.packed_current_base.v1` | — | 1 row / 90 B |
| `0x00060001 changelog.commit` | 1 row / 117 B | 23 rows / 3,043 B |
| **TOTAL** | **42,471,320 B** | **313,414 B** |

220,040 hot rows ≈ 22 complete generations × 10,000 rows: every mutation left its
predecessor generation behind, and nothing reclaimed it (§2.7). The tracked lane
stored 23 commits, 24 changes, the changelog, the GC reachability deltas, the
columnar row groups *and* the current state in **0.7%** of the bytes.

Note also what is *missing* from the untracked column: no
`entity.columnar_row_group_column`, no `packed_current_base`, no
`commit_delta_segment`. That is the independent confirmation of §3.3's
attribution — with untracked rows present, those optimizations are never built.

### 4.4 Code cost

PR #1273's `−10,068` is against `main@1d406823f`. Split by category:

| category | files | + | − | net |
|---|---|---|---|---|
| `packages/lix/src` (production engine) | 71 | 1,506 | 6,653 | **−5,147** |
| tests | 30 | 232 | 1,524 | −1,292 |
| benchmarks | 9 | 15 | 1,570 | −1,555 |
| `rfcs/001-preprocess-writes` | 1 | 0 | 315 | −315 |
| docs / other | 5 | 6 | 6 | 0 |

Verified against the current `claude-3-optimizations` tree: 66 files under
`packages/lix/src` mention `untracked`; ~1,127 production occurrences and ~810
in-file `#[cfg(test)]` occurrences; 251 exact `lixcol_untracked` references
repo-wide. Two files exist only for untracked:
`packages/engine-benchmarks/benches/untracked_state_crud/main.rs` (1,460 lines,
plus a 392 KB fixture) and
`packages/lix/tests/integration/sql/untracked_current_state.rs` (504 lines).

−5,147 net production lines is ~1.6% of `packages/lix/src`, but the removals are
concentrated in the files that dominate every other optimization on this branch:
`transaction/commit.rs` (−1,119), `live_state/tracked_head/hot.rs` (−973),
`transaction/validation.rs` (−778), `gc.rs` (−454), `engine.rs` (−388),
`live_state/context.rs` (−310).

Non-Rust: **14 total mentions repo-wide.** There is essentially no user-facing
documentation of untracked left — `docs/` mentions it in exactly one sentence
(`docs/history.md:151`).

### 4.5 What the lane costs users, in features

Not a number, but load-bearing:

- **Undo/redo refuses to run.** `session/undo_redo.rs:497-541`:
  `"cannot undo/redo commit '{target}' because removing its file or directory
  would delete untracked state"`.
- **Mixed writes are rejected.** `transaction/staging.rs:4076-4142`:
  `"cannot mix tracked and untracked writes for schema '{}' entity_pk '{}' in
  branch '{}' within one transaction; commit or roll back before changing
  durability"`.
- **Promotion is impossible.** `lixcol_untracked` is insert-only
  (`registry.rs:597`); a row cannot be promoted from ephemeral to durable
  in place (`tracked_head.rs:597-601` is an explicit fence against it).
- **Identity collisions are a hard error.** `commit.rs:4705-4712`:
  `"cannot insert tracked row … a canonical untracked row already exists;
  delete it first"`; `hot.rs:8119-8141` errors `CODE_UNIQUE` on a shared identity.

---

## 5. A / B / C

### 5.1 The three designs

- **A — status quo.** Two serving roots per branch, one bit per row, dual reads,
  complete-generation republish on every untracked write.
- **B — one plane, keep the marker.** Untracked rows live in the tracked plane
  with real `change_id`/`commit_id`; the marker drives SQL filtering and history GC.
- **C — remove the concept.** PR #1273: no `lixcol_untracked`, no second root;
  retain only exact global deterministic engine rows and exact canonical branch
  lifecycle rows as non-historical current-state facts, failing closed at
  staging, preservation and snapshot boundaries.

### 5.2 Against the layout-invariants checklist

| invariant | A | B | C |
|---|---|---|---|
| **1 Single authority** | ✗ — "two independent authority domains" (`hot.rs:5426`), two serving roots in `BranchHeadControl`, dual probes in reads *and* in uniqueness validation | ✓ — one plane, one root | ✓ — one plane, one root, and the marker deleted too |
| **2 Commit canonicality** | ✗ — `untracked_generation` is a `CommitId`-shaped value that is **not** a commit: no parent links, no state root, absent from `tracked_reachability()` (`control.rs:88-101`) | ✓ — by construction | ✓ |
| **3 Derived views are caches** | ✗ — the untracked generation is not rebuildable from any canonical record; it *is* the only copy | ✓ | ✓ |
| **4 Atomic publication** | ✓ — the control swap is atomic | ✓ | ✓ |
| **5 GC from refs** | ✗ — untracked payloads need a *disconnected* retention side-table (`0x0002_0002`) which production GC never drains (§2.7) | ~ — a marker still needs a second retention rule, but reachability stays one implementation | ✓ — one reachability implementation |
| **6 SQL reads canonical records** | ✗ — reads must merge two roots and the columnar/base caches are disabled | ✓ | ✓ |

A fails four of the first five. That alone satisfies the runbook's acceptance
gate clause 2 ("a cut that removes dual/multi authority"), and §3–4 show the cut
is not slower.

### 5.3 Evaluating B honestly — the owner's hypothesis

B is the right instinct and it deserves a straight answer on each awkward part.

**"Do untracked rows get a real `commit_id` — and does that force a commit on
every fast mutation?"** Yes, and yes. Today an untracked write publishes a
control swap plus a hot generation and writes **no** commit record and **no**
change row (`commit.rs:1908-1924`: `let commit_id = if row.untracked { None }
else { … }`). Under B it must write a `changelog.commit` row with parent links
and a canonical state root, a `changelog.change` row, a `commit_change_id`, a
`commit_state_manifest`, a `commit_mutation_catalog` and a GC reachability delta
— i.e. exactly what the tracked lane writes. §4.3 measures that full apparatus
at **313 KB and 280 µs per mutation at 10k rows**, versus the untracked lane's
42.5 MB and 11.1 ms. **B's write path is 40× faster and 135× smaller than A's.**
The intuition that "a real commit id must be more expensive" is measurably
backwards on this engine.

**"Does 'one commit id = full state' conflict with excluding untracked rows from
history GC?"** It resolves it. Today the conflict is structural: the untracked
payload is reachable from nothing in the commit graph, which is precisely why
`0x0002_0002` exists as disconnected retention metadata — a violation of
invariant 5 — and why the production sweep silently omits it. Under B every row
is reachable from a commit, so there is **one** reachability implementation, and
"GC history for fast-mutating rows" becomes a *retention policy over reachable
history* (prune old commits for schema X), which is the same mechanism
checkpoints already use. That is strictly better than a second lane.

**"What happens to undo/redo, checkpoints, and merge?"** They all get simpler,
because every one of them currently has a hand-written untracked exception:
`reject_untracked_descriptor_cascade` (`session/undo_redo.rs:497`),
`checkpoint.rs:54` hardcoding `untracked: false`, `merge_final_untracked_rows`
(`hot.rs:8119-8141`) erroring on shared identities,
`reject_lifecycle_retention_collisions` (`hot.rs:8167`), the working-diff
baseline fence (`tracked_head.rs:1666-1670`). Under B a marker-carrying row is an
ordinary row: it undoes, checkpoints and merges normally. The *only* thing the
marker changes is whether a query or a diff chooses to display it.

**So what is left of B that is not C?** One boolean column, with these
properties: it is not part of identity, it does not change storage, it does not
change reachability, it does not change conflict resolution, and it is only ever
read by a `WHERE` clause. **That is not an engine feature — it is schema data.**
An application that wants `ui_state` rows excluded from its diffs can register
them under their own schema key and write
`WHERE schema_key <> 'ui_state'`, or carry its own boolean in its own snapshot.
Keeping `lixcol_untracked` as an engine column buys a shorter predicate at the
cost of a public API surface, 251 references, a per-row encoder guard, and a
permanent invitation to reintroduce lane-specific behaviour behind it. Under the
runbook's "90% happy path" rule, that is not worth it.

Therefore: **B collapses into C.** Adopting C loses nothing the owner asked to
keep, provided the removal keeps the diff surface filterable — which it does,
because a diff is already a SQL relation.

### 5.4 What C actually loses

Stated plainly, because the brief asks:

1. **The name.** Applications currently writing `lixcol_untracked: true` must
   change. This is a breaking public API change; PR #1273 is explicit that there
   is no alias, no fallback reader, no migration decoder, and that existing
   repositories with untracked rows must be exported or reinitialized.
2. **"Writes that create no history at all."** After C, every entity write
   creates a change row (`docs/history.md` diff in #1273:
   `-Ordinary untracked writes do not create change rows.` /
   `+Every ordinary entity write creates a change row.`). For a sidebar-width row
   updated on every drag, that is more history rows than today — but §4.2/§4.3
   show it is 40× less time and 135× fewer bytes than the current untracked path,
   so "creates no history" was never the cheap option.
3. **The `WHERE lixcol_untracked = FALSE` shorthand.** Replaceable by schema-key
   filtering. If the owner wants the shorthand back, it can be reintroduced later
   as a pure schema-level annotation with no engine plane behind it — and that
   decision is then independent and reversible.

C explicitly **keeps** the two hard cases that genuinely cannot be historical:
"exact global deterministic engine rows and exact canonical branch lifecycle
rows as non-historical current-state facts; both fail closed at staging,
preservation, and snapshot boundaries" (PR #1273 body). That is the 10% edge
case handled by failing closed, per the runbook.

---

## 6. Recommendation

**Adopt C. Rebase `codex/remove-untracked-state-stack` (`c017cea62`) onto
`claude-3-optimizations` and land it as a cut PR.**

Justification against the acceptance gate, clause 2 — a cut that removes dual
authority, shown not to be slower:

- Removes the second serving root (`BranchHeadControl.untracked_generation`) and
  with it the only place in the engine where a hot-path comment has to describe
  "two independent authority domains".
- Removes disconnected retention metadata (`0x0002_0002`) that production GC
  never drains, restoring one reachability implementation (invariant 5).
- Removes ~5,147 net production lines from the six files that every other
  optimization on this branch has to touch.
- Is **not slower**: it deletes a path measured at **39.7× slower per mutation**,
  **135× larger on disk**, and **+366–538% on entity scans**, and it re-enables
  the columnar and immutable-base projections unconditionally.
- Costs point reads at most the −3.8%…−7.2% currently observed *in the presence
  of untracked rows* — which is a gain the lane only produces by destroying the
  scan path, and which does not exist in the untracked-free configuration that
  becomes universal after the cut.

Do **not** build design B as a separate step. B's only residue over C is a
boolean column, and if that column is wanted it should be added afterwards as a
schema annotation with no engine behaviour attached — not as a reason to keep the
lane alive through another refactor.

### Main risk

**The rebase, not the design.** PR #1273 is based on `main@1d406823f`;
`claude-3-optimizations` has 126 commits since that merge base, and **53 of the
116 files #1273 touches have also changed on this branch** by +2,561/−781 lines —
including `transaction/commit.rs`, `live_state/tracked_head/hot.rs`, `gc.rs`,
`engine.rs`, `live_state/context.rs` and `transaction/staging.rs`, the exact files
where both the cut and this branch's hot-row/packed-base work live. A mechanical
rebase will conflict heavily and, worse, can silently drop this branch's
optimizations while resolving.

Mitigation: do not `git rebase`. Re-derive the cut on top of
`claude-3-optimizations` using `c017cea62` as the specification, and gate on
`cargo test -p lix --all-features` (mandatory — `storage_bench.rs` is behind
`storage-benches`), plus `cargo clippy --workspace --all-targets --all-features
-- -D warnings` and `cargo check -p lix_js_sdk --target wasm32-unknown-unknown`.
Then re-run `tracked_state_crud` `read_all_rows_consumed`, `insert_all_rows` and
`update_all_rows` against `~/claude3/base` to confirm the tracked path did not
regress; the untracked benches will no longer exist to compare.

A secondary risk: PR #1273's own claim is structural, not empirical — it opens
with *"This PR makes a structural simplification claim, not an invented
throughput claim."* This analysis supplies the missing numbers, so the cut can
now be justified on measurement as well as on authority.

---

## Appendix — artifacts

| what | where |
|---|---|
| read-tax A/B driver | `~/claude3/expY-readtax.sh` on `ryzen-9950x-IV` |
| read-tax raw log (45 arm-runs) | `~/claude3/expY-readtax.log` |
| read-tax parser | `scratchpad/parse_readtax.py` |
| mutation/byte probe (committed) | `packages/engine-benchmarks/examples/expY_untracked_mutation_scaling.rs` |
| bench binary used for reads | `~/claude3/base/target/release/deps/tracked_state_crud-dfe69a0deced0469` |
