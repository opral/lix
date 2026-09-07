# Releasing Lix

1. While the release PR is a draft, pushes to main update its version metadata
   and changelog. The generator validates metadata before writing; the separate
   **Release metadata** workflow also validates draft PR events without building
   the engine.
2. Mark the release PR ready. This freezes automated updates and starts normal
   CI. The bot will neither update it nor close it in favor of another version.
3. Wait for **Release ready** on the current head commit. This aggregates the
   existing CI suites, rejecting failures, cancellations, and unexpected skips.
   It also checks that the PR is still open, ready, and at the tested head.
4. Merge the candidate. The existing publishing workflow handles publication.

To include newer changes, return the candidate to draft and manually dispatch
**Release PR** from main. Review the refreshed diff, then mark it ready again.
Do not rerun an old draft event to validate a ready candidate: reruns retain
their original event payload. Rerun the ready-for-review run instead.

## One-time rollout

After these workflows land, configure main's branch protection to require
**Release ready** from GitHub Actions, preserving any other required checks.
GitHub requires checks on the target branch, so this lightweight aggregate is
also emitted for ordinary ready PRs; it adds no test suite or build matrix.
Draft CI uses a different check name and concurrency group, so it cannot cancel
ready validation or overwrite the required check with a skipped result.

Existing release PRs created before this change must be returned to draft and
refreshed through **Release PR** once to pick up the workflow changes. Do not
merge based on an older green run that skipped validation. Adding the job to YAML
alone does not make it a required GitHub check.
