# Immutable additive nine-seam binding

This v3 package losslessly binds the independently authored additive closure
oracle. The referenced object is immutable; this file is not a replacement for
its source or a moving-ref claim.

```text
ref origin/codex/review/fd2-file-history-closure-oracle-b484
head 9cd14f684205f21f76f0504871fd00ed2d5eea07
tree c896aa3dc52e8ccf92778a1528157e5a5adeb9fe
parent 929c0da6a3474804564eb21f08c5fddeb029bf72
b484..head full-index 05a940a70d4e57015fa357a4eb9517d2fd8aa0f4057c8dbae42d66cda5141578
patch e247829f0597bdced8d9cc340fae145ec8012d35
calibration log 6f74dbaa54574e6e94dec6f758c1a6d2047225d7f7bfe31e1f50582f2426e832
SHA256SUMS 8637ee3603ec6f502c714e00cd34916ac4fda3da0fb1ec0aa26638a3a18a1311
REPORT.md 007b86a9e5ed8e476f2028fef2bc8cb92bcd3eb4397341cb579b200e0eb265c1
```

The referenced artifact checksums are:

```text
abdeb91ed3d2e32a9d01cedcb9dd9bbd62529144417f412189d21c6323c45db9  CLOSURE_ORACLE.md
b607b552e386b9308c72376b6d5180ae129653fd1419cc20734de4a73b479cf4  README.md
007b86a9e5ed8e476f2028fef2bc8cb92bcd3eb4397341cb579b200e0eb265c1  REPORT.md
d94466d41a79c7149f84871dddf2fd6ebadd322319075c983cb9ef0d02c1a1f2  model_oracle.py
10a787cc406764685bbc18d239cf1a152fc17c1b274b66291be0920a1e27a5d9  run_oracle.sh
16ce589a67e9cc4a209ead825ba58093ab8215d18ec690861b5a8cce9d6f9fed  source_gate.py
```

Exact replay was run against a detached b484 checkout with the exact
referenced package's `run_oracle.sh`:

```text
bash test-reports/forktree-stage2-fd2-closure-oracle/run_oracle.sh <detached-b484-checkout>
```

Replay result: exit `0`, 27 lines, 1,033 bytes, combined stdout SHA-256
`6f74dbaa54574e6e94dec6f758c1a6d2047225d7f7bfe31e1f50582f2426e832`.
It reports all nine RED labels, the working-diff positive control, all model
controls, `MODEL_STATUS=GREEN`, and
`ORACLE_STATUS=GREEN_EXPECTED_RED_SOURCE_CALIBRATION`.

This binding is package-only. It does not import, compile, or execute Lix
production code and does not alter the referenced oracle or any production
path.
