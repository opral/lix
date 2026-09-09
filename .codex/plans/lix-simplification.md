# Lix simplification integration

This draft integration branch collects independently reviewed changes before final review against main. Physical-layout migrations remain supported; public API compatibility is not required.

- [ ] Retire unused JSON runtime storage, fences and hashing; retain required migration decoding.
- [ ] Finish typed INSERT lowering and remove custom JSON recertification.
- [ ] Share one bound read access plan across native reads and SQL providers.
- [ ] Stage branch lifecycle through typed control intents.
- [ ] Remove the redundant TypeScript protocol export.
- [ ] Remove unused BranchEquals storage preconditions.
- [ ] Fix shared row/mutation contract ownership.
- [ ] Centralize transactional plugin publication and cleanup.

Each implementation PR targets this integration branch and starts in draft. Validate and merge each child PR into this branch. The integration PR remains draft until final user review; do not merge it into main automatically.

Required validation includes engine all-simulations and doctests, adapter conformance for storage-contract changes, and targeted SDK, migration, SQL, branch and plugin tests. Preserve native read and batch INSERT fast-path performance.
