# R4 correction calibration

The corrected verifier was run from the exact predecessor worktree with no
arguments:

    bash test-reports/trackedhead-sql-deletion-plan-b59/verify_trackedhead_sql_deletion_plan.sh

Pinned anchors were b59
b59e1f11a51153e0a787a81f0f25bf104d150aaf and oracle v2
1d9c47728377c6ec7d2646704d51f3aadb11c773. The expected source-deletion result
is RED because the predecessor still contains tracked-state, branch-control,
mutation-revision, and commit-manifest owners. Exit status was 1.

Captured log: r4-correction-calibration.log
SHA-256: 3f02ee55aec3cc2d5617bee93d31b5113795dff71c30ddd110ebd75a026af65e
Lines/bytes: 336/34693.

The verifier writes temporary residue output inside the candidate worktree,
not /tmp, so the gate remains runnable when the host temporary filesystem is
full.
