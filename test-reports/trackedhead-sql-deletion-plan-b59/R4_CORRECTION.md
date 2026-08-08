# R4 deletion-plan correction

The predecessor plan at 3763667531069aa6a60fb7d3469de45ba329450c omitted
these concrete production owners:

- checkpoint.rs TrackedStateStoreReader parameters;
- session/execute.rs BranchHeadControlContext and commit-state manifest loads;
- branch/refs.rs stage_branch_head_control;
- branch control module/cache and BRANCH_HEAD_CONTROL_SPACE;
- commit_graph/context.rs and engine.rs tracked-state commit manifest ownership;
- storage_adapter/context.rs mutation-revision reads, preconditions, and
  writers;
- storage_adapter/spaces.rs MUTATION_REVISION_SPACE and
  TRACKED_MUTATION_REVISION_SPACE;
- observe/session/transaction mutation-revision consumers.

They are now in SOURCE_INVENTORY.tsv and COMPILER_WAVE.tsv. Every direct SQL
and history consumer has a concrete ForkTree migration owner or a typed
fail-closed deletion outcome. A retain-blocker without one of those actions is
not accepted.

The verifier now has no parameters. It always checks the fixed b59 and v2
anchors and rejects branch-control, stage-writer, mutation-revision, and
tracked-state-manifest residues. The required order places the selector/epoch
control fence first, reader migrations next, mutation-revision deletion after
its observers move, and physical owner deletion before the first accepted
compile.

The independently frozen corruption discriminator is bound as R4 head
7ff277c297e93eba83da09bf12f83d6485a8458b, tree
a0e6be2c9029144497b75e4f9dcd6b001d71fec9, full-index diff
55d018ea5389898414dbf7844053c5339b316bf36652574b86983c1c8cb43b4b, patch
1ad9a7cc93f5386920032d3d1b1cebc8febaa43d.
