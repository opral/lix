# fd2 source-gate calibration

This is a deliberate RED calibration against the immutable production anchor;
it is not a production test result and does not authorize implementation.

## Exact invocation

```text
test-reports/stage2-sql-change-reader-fd2/verify_source_contract.sh \
  /root/repos/lix-forktree-stage2-a12-production \
  fd2be256d763f17e9f127d4c984e36fba191cb82
```

Observed exit status: `1` (expected).

Combined stdout/stderr: 23 lines, 1557 bytes, SHA-256
`74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5`.

The RED is causal, not a token-only calibration: fd2 still passes a raw
`query_source.store` through the SQL provider, invokes the deleted-owner
tracked-state scanner, constructs independent Changelog/CommitGraph readers,
and performs a direct change-space point read. It has no
`&query_source.forktree_reader` scan/exact call arguments, no provider-owned
duplicate-ID rejection before sort/limit, and no ChangelogQuerySource
ForkTree-reader field. The fixture table itself is complete and validates
the required positive and negative cases.

The inherited compiler logs are recorded without rerunning a build:

| scope | result | log SHA-256 |
| --- | --- | --- |
| fd2 library | 136 errors / 9 warnings | `41cf9efa8a35279a05f58a0a3da8b2fd0ddb41a542c0c033d0f8714bed11f6a0` |
| fd2 test-aware | 378 errors / 15 warnings | `3dd31c3da5333e39364ca5b579ad913eeb516ddb4f756a2cad190ab17a93d12a` |

No runtime, adapter, benchmark, production edit, or candidate implementation
was run for this package.
