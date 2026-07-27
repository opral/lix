# Change proposals

A change proposal is a durable, repository-global review request from one
branch into another. It is designed for the common agent workflow:

1. An agent creates a branch and works asynchronously.
2. The agent proposes its completed branch to a human-owned target branch.
3. The human reviews a semantic diff, then accepts or rejects it.

The proposal metadata is a **tracked global** entity because it describes a
relationship between two branches. It is never tracked as content on either
the source or target branch, so creating or resolving a proposal cannot show
up in the work being reviewed.

## API

The initial implementation is available through the engine, Rust SDK, and
read-only SQL review surfaces.

```rust,no_run
use lix_sdk::{
    BranchDiffOptions, CreateChangeProposalOptions,
};

# async fn example(lix: &lix_sdk::Lix) -> Result<(), lix_sdk::LixError> {
let proposal = lix.create_change_proposal(CreateChangeProposalOptions {
    id: None,
    source_branch_id: "agent-draft".into(),
    target_branch_id: "main".into(),
}).await?;

let review = lix.change_proposal_diff(&proposal.id).await?;
if review.is_accept_ready {
    let accepted = lix.accept_change_proposal(&proposal.id).await?;
    println!("target now heads at {}", accepted.merge.target_head_after_commit_id);
} else {
    lix.reject_change_proposal(&proposal.id).await?;
}

let direct_review = lix.branch_diff(BranchDiffOptions {
    source_branch_id: "agent-draft".into(),
    target_branch_id: "main".into(),
}).await?;
# Ok(())
# }
```

`BranchDiff` and the `review` member of `ChangeProposalDiff` contain
identity-level entries with the same useful shape as `lix_working_change`:
`entity_pk`, `schema_key`, `file_id`, change kind, and before/after change
ids. They also include source/target/base commit ids, merge outcome, stats,
and conflicts. Payload hydration remains lazy, so opening a review does not
copy all changed content into a third change set.

## SQL surfaces

`lix_change_proposal` is a normal, global Lix entity with a read-only SQL
surface. It is the authoritative proposal record. Its lifecycle must go
through the proposal API; private controls hold only an id reservation and an
open-pair CAS mapping, never a second copy of proposal metadata.
Because the row is global rather than branch-owned, it deliberately has no
misleading `lix_change_proposal_by_branch` projection.

```sql
SELECT id, state, source_branch_id, target_branch_id,
       source_head_commit_id, target_head_commit_id
FROM lix_change_proposal
WHERE state = 'open';
```

The review surfaces require an exact source/target pair. That makes their cost
one merge-base calculation and one three-way analysis, rather than an
accidental all-branches cross product:

```sql
SELECT entity_pk, schema_key, file_id, change_kind,
       before_change_id, after_change_id,
       base_commit_id, source_head_commit_id, target_head_commit_id
FROM lix_branch_diff
WHERE source_branch_id = ?
  AND target_branch_id = ?;

SELECT conflict_kind, entity_pk, schema_key, file_id,
       target_change_kind, source_change_kind
FROM lix_branch_merge_conflict
WHERE source_branch_id = ?
  AND target_branch_id = ?;
```

Both surfaces use `diff(merge_base(source, target), source)`. The first is
working-change shaped; the second contains the merge conflicts that would
block acceptance. They repeat the observed commit pins on each result row so
a review can retain the exact snapshot it rendered. They are a review of the
*current* branch pair; `change_proposal_diff(id)` is the frozen proposal
review, and never silently follows later branch heads. An empty diff naturally
has no row on which to carry those pins; callers that need an always-one-row
summary should use the API result for now.

## Review semantics

The review patch is **not** a raw state comparison from target to source.
It is the source branch's authored contribution:

```text
base = merge_base(source_head, target_head)
review patch = diff(base, source_head)
merge preview = apply(review patch to target_head)
```

This is Git's three-dot review meaning. It excludes target-only work while
still reporting conflicts that would prevent acceptance. A raw
`diff(target_head, source_head)` is still useful for a state comparison, but
is not an honest answer to “what work is this agent proposing?”

## Snapshot and lifecycle contract

At creation, Lix stores:

```text
ChangeProposal {
  id,
  source_branch_id, target_branch_id,
  base_commit_id,
  source_head_commit_id, target_head_commit_id,
  state: open | accepted | rejected,
  accepted_target_head_commit_id?
}
```

Lifecycle timestamps and provenance are not duplicated on the entity: Lix
records them in the tracked changes that create and update the proposal. Query
`lix_change` for workspace-wide activity, or derive a more specific audit
projection from change history.

The source and target branch-head controls are observed and compare-and-swap
guarded in the same commit that creates the proposal. This prevents a proposal
from being published as a coherent snapshot after one of its branches already
moved.
The reserved `global` control branch is not a valid source or target.

Accept is similarly one atomic operation:

1. Require the proposal to be open and run on its target branch.
2. Require source and target heads to still equal the pinned heads.
3. Perform the pinned three-way merge into the named target.
4. Mark the proposal accepted and release its open-pair index.

If a source or target branch had already moved when acceptance begins,
acceptance returns `LIX_CHANGE_PROPOSAL_STALE` and changes neither branch nor
proposal state. If it races after the final observation, the storage CAS
aborts atomically with `LIX_TRANSACTION_CONFLICT`; it likewise changes neither
branch nor proposal state. Create a replacement proposal from current heads
and review that snapshot.
The Rust SDK opens the target session internally, so SDK callers do not need
to switch their workspace branch to accept.

Reject is intentionally smaller: it changes `open` to `rejected`, releases
the one-open-proposal-per-ordered-pair index, and preserves the source branch
and durable record. A rejected agent can revise the branch and submit again.

Open proposal pins are GC roots. This keeps a reviewed source commit
addressable even if its branch subsequently advances while the reviewer is
away.

## Choices and trade-offs

| Choice | Benefit | Deferred cost |
| --- | --- | --- |
| Frozen heads | The reviewer sees exactly what acceptance will apply. | The agent makes a replacement proposal after new commits. |
| Tracked global entity | No accidental ownership by source or target; lifecycle is auditable and SQL-readable. | Proposal lifecycle makes a small global control-branch commit. |
| Merge-base source diff | Shows authored work, not unrelated target changes. | A distinct raw state-comparison API may be added later. |
| Strict target/source CAS | Simple, linearizable accept without surprise rebases. | No automatic rebase or merge queue. |
| One open source→target pair | A simple agent idempotency/default review rule. | Parallel alternatives use separate branches or wait for resolution. |

## Deliberate v1 non-goals

- Mutable PR-style source-head updates and proposal-version history.
- Inline comments, approvals, threaded discussion, or suggested partial hunks.
- Automatic rebases, merge queues, stacked-change dependency handling, and
  conflict-resolution UIs.
- Proposal comments/history and an explicit global-head SQL history route.
  The current entity is tracked and durable, but a generic active-branch
  history view would be misleading for global control-branch commits.

These are valuable once the basic asynchronous agent-to-human handoff proves
out, but they complicate the 90% path substantially.

## Why this shape

- Git's `request-pull` describes a request from a known base to a source tip,
  and Git's compare-and-swap ref update provides the acceptance concurrency
  precedent. [Git request-pull](https://git-scm.com/docs/git-request-pull),
  [Git update-ref](https://git-scm.com/docs/git-update-ref)
- GitLab records base and head SHAs for merge-request diff versions and
  distinguishes merge-base diffs from target-head merge diffs. [Merge request
  API](https://docs.gitlab.com/api/merge_requests/), [diff
  concepts](https://docs.gitlab.com/development/merge_request_concepts/diffs/)
- Google Docs keeps suggestion lifecycle separate from document content and
  offers strict revision checks when callers need review fidelity. [Suggestions
  API](https://developers.google.com/workspace/docs/api/how-tos/suggestions),
  [revision control](https://developers.google.com/workspace/docs/api/reference/rest/v1/documents/batchUpdate)
- Yjs update diffs are state-vector synchronization deltas: useful for
  collaborative transport, not a human review patch. [Yjs document
  updates](https://docs.yjs.dev/api/document-updates)
- Jujutsu separates stable change identity from movable bookmarks, reinforcing
  the choice to keep proposal identity distinct from branch pointers. [Jujutsu
  glossary](https://docs.jj-vcs.dev/latest/glossary/)
- Sapling's stacked-review workflow demonstrates the value of dependent review
  chains, but also why stacks stay out of this first API: one independent
  source→target proposal is the reliable default before dependency ordering
  and restacking rules are introduced. [Sapling stacks](https://sapling-scm.com/docs/git/sapling-stack/)

## Prototype validation

The integration suite covers a pinned directional diff, divergent atomic
acceptance, rejection/source retention, and stale-target refusal. The focused
test target completed 14 tests, including the SQL pair surface and
merge-conflict surface.

For the review cost, a release RocksDB probe seeded 10,000 rows and made 1,000
ordinary commits with 10 writes each. Eleven warm samples on this development
machine produced the following results:

| History shape | Effective review rows | Historical diff core p50 | Existing `lix_working_change` SQL p50 |
| --- | ---: | ---: | ---: |
| Same 10 rows rewritten in every commit | 10 | 4.839 ms | 0.814 ms |
| 10 distinct rows written in every commit | 10,000 | 65.824 ms | 33.104 ms |

`measure-history` invokes the shared tracked-state diff primitive used by the
fast-forward/static-target branch-review path (`diff(base, source)`). It
intentionally excludes merge-base lookup, target-side conflict analysis, and
DataFusion planning, so it is a useful lower-bound/cost-shape probe rather
than an end-to-end `lix_branch_diff` service-level benchmark. The two shapes show the desired
property: replaying 1,000 commits is inexpensive when the output collapses to
10 identities, while returning 10,000 identities is predictably dominated by
result construction. Proposal creation never materializes that third copy.

The benchmark is repeatable:

```bash
cargo bench -p lix_engine_benchmarks --features storage-benches \
  --bench tracked_working_diff -- \
  setup /tmp/lix-proposal-diff repeated 10000 1000 10

cargo bench -p lix_engine_benchmarks --features storage-benches \
  --bench tracked_working_diff -- \
  measure-history /tmp/lix-proposal-diff <base-commit> <source-head> 21
```

The key data-structure trade-off is structural: proposal creation/resolution
adds one small tracked global entity row plus two private point controls
(id reservation plus open-pair index), regardless of the number of changed
entities. The controls never duplicate proposal payload. Review cost remains
proportional to the changed subtrees exposed by Lix's hash-guided tracked-state
diff, rather than copying the proposal into a third materialized change set.
