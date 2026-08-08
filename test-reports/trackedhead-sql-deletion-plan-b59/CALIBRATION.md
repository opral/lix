# Source-only calibration

The verifier was run with no compile, benchmark, adapter, or production
mutation:

    bash test-reports/trackedhead-sql-deletion-plan-b59/verify_trackedhead_sql_deletion_plan.sh

The source anchor is exact b59
b59e1f11a51153e0a787a81f0f25bf104d150aaf, and the corruption acceptance
anchor is the approved six-domain oracle v3 head
33aa59975808099dfb5e9ca675a1633d713dccf3. The expected RED is preserved:
the superseded tracked_state/context.rs and tracked_state/diff.rs remain in
the unwired source frontier. The normalized deterministic verifier output
SHA-256 is 8145298114e65861611012473a1bd195dd496a53a01224206e20f260bbaab77e,
and the process exit status is 1. Normalize the captured output by removing
only the dynamic `anchor=... oracle=... head=...` line before hashing:

    sed '/^anchor=.* oracle=.* head=/d' verifier-output.log | sha256sum

The normalized output includes the exact PLAN assertion
`24 cases = 6 domains × 4 corruption modes` before the expected residue RED.

The v3 stateful model is the required six-domain corruption contract: all
malformed, missing, wrong-kind, and identity-substituted selector/root cases
must produce one retained read followed by zero plan/write/commit/rotation.
It is test/report evidence, not a runtime approval.

The corrected deletion verifier is intentionally invoked with no arguments.
It pins b59 and validates the exact v3 commit/tree/parent/diff/patch/package
identity from the manifest, preventing a candidate from masking residue or
weakening the corruption contract by supplying alternate anchors.
