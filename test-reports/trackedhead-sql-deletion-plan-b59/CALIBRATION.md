# Source-only calibration

The verifier was run with no compile, benchmark, adapter, or production
mutation:

    bash test-reports/trackedhead-sql-deletion-plan-b59/verify_trackedhead_sql_deletion_plan.sh \
      /root/repos/evidence/tracked-head-whole-module-oracle-b59-corrected-v2

The target was exact oracle-v2 head
1d9c47728377c6ec7d2646704d51f3aadb11c773. The expected RED is preserved:
the superseded tracked_state/context.rs and tracked_state/diff.rs remain in
the unwired source frontier. The captured log is
v2-calibration.log, SHA-256
4203cfb771b58cab263c0235db69a248e08c569a80bedfc2044d1713756c0196, and the
process exit status is 1.

The v2 stateful model remains 7/7 for its covered semantics, but is not a
runtime approval because selector/catalog/checkpoint corruption domains were
not independently mutated. A separate v3 test/report-only successor is
required before any implementation promotion. That v3 is now bound in the
manifest at 33aa59975808099dfb5e9ca675a1633d713dccf3.

The corrected deletion verifier is intentionally invoked with no arguments.
It pins both the b59 source anchor and the v2 oracle internally, preventing a
candidate from masking residue by supplying alternate anchors.
