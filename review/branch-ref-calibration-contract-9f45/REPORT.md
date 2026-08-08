# BranchRef calibration reproducibility contract

Status: **PASS for the canonical RED calibration contract**. This is
test/report-only evidence; it is not a production acceptance result.

## Immutable inputs

Base b59:

```text
commit b59e1f11a51153e0a787a81f0f25bf104d150aaf
tree   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
```

Candidate:

```text
commit 9f45f77955317b8dd64fadb049d85c33ca109bf4
tree   c38c4d60c74bf70994378029ad9e286a83cf2d69
parent ee00381fd95148cd85a4c0940c3c17ee6805aa25
parent..head full-index binary diff b77a739ac6231e3fac859bb80a4d38b968f5cb911aaca1f88644e20996953b37
stable patch-id 872cb7d3d4e7756ca895119ec0ebdee13aa1717a
HANDOFF SHA-256 288926d43355526489908c84845ba2d30343e97117f04652f0d58754862c128b
```

The HANDOFF digest is also embedded in `MANIFEST.json`.

## Exact replay

From the candidate worktree:

```sh
bash review/branch-ref-calibration-contract-9f45/verify_calibration.sh \
  /tmp/lix-b59-branch-ref-source-1786195722 \
  /tmp/lix-branch-ref-calibration-contract-1786195688
```

The verifier independently checks both commit/tree identities, invokes the
candidate-owned scanner with the fixed b59 anchor, captures stdout and stderr
separately, and requires the scanner's expected RED exit status.

Normalization is implemented by the committed
`normalize_branch_ref_scan.sh` (SHA-256
`1536d8146f87a4fc26a440789eb08a1825404c40bb517ccbb49ef32f6ff66678`):

- CRLF becomes LF;
- trailing horizontal whitespace is removed;
- only the absolute `branch-ref-whole-closure root=...` value becomes
  `root=<ROOT>`;
- line order, paths, inventory, counts, and all other bytes are preserved;
- stdout and stderr remain separate, then are wrapped as `[stdout]` and
  `[stderr]` before hashing.

Expected canonical result for both immutable inputs:

```text
required-missing=0
legacy-residue=460
old-closure-paths=4
lix-branch-ref-occurrence-files=15
non-derived-lix-branch-ref-files=4
authority-use-lines=331
normalized bytes=78700, lines=26
normalized SHA-256=026fcd6b7aaa9afd8341fdca6451962d4addd5aedef63724b6f90d50b8b573bb
stderr SHA-256=e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

Observed raw stdout hashes are path-dependent and are retained only for
diagnostics:

```text
b59       6933de460c3f67c773b0dfbd04012fe091278c24bf2963c076b6faeef9e9b725
candidate 9113ab96180b93e406ababca0b73007cf8299cde15d285aa20ead19cae6ff55d
```

Both normalized hashes match exactly. The script output is the expected
compiler-red BranchHead/BranchRef closure boundary; no production compile,
adapter runtime, or source mutation was performed.

## Alternative-calibration rule

The prior 481/343 claim is not accepted as a presentation-only variant. Any
future alternative must commit its normalizer/source, raw captures, normalized
output, expected hash, immutable b59/candidate identities, and a manifest that
embeds the HANDOFF digest. It must reproduce from these exact trees. A changed
count without a committed, reproducible source transformation is a calibration
failure.
