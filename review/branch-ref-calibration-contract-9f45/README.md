# BranchRef calibration reproducibility contract

Test/report-only evidence anchored to immutable b59 and the immutable 9f45
BranchRef correction oracle. This package does not change `packages/lix` and
does not run production compilation, adapter tests, or runtime qualification.

The contract makes the source-gate calibration reproducible:

1. Invoke the exact candidate-owned `verify_branch_ref_whole_closure.sh` on
   the exact b59 source tree and exact 9f45 tree with the fixed b59 anchor.
2. Capture stdout and stderr separately; do not merge streams.
3. Normalize only CRLF to LF, remove trailing horizontal whitespace, and
   replace the first absolute `root=` value with `<ROOT>`. Preserve line
   order, inventory lines, paths, counts, and all other bytes.
4. Wrap the normalized streams as `[stdout]`, normalized stdout, `[stderr]`,
   normalized stderr and hash those exact bytes with SHA-256.
5. Require both immutable trees to produce the canonical 460/331/4 result.

An alternative 481/343 claim is not admissible unless its normalizer/source,
raw captures, normalized output, and hashes are committed with the claim and
the same two immutable inputs reproduce it. The HANDOFF digest is a required
manifest field, not an out-of-band assertion.
