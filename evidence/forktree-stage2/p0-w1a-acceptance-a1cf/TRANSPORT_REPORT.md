# Immutable transport report

This directory transports the four frozen P0+W1a package files losslessly from
`/root/repos/lix-evidence/stage2-production-review-a12b/p0-w1a-acceptance-a1cf`.
`TRANSPORT_REPORT.md` is transport metadata and is not included in the frozen
four-file package manifest.

## Exact package identities

| Frozen file | SHA-256 |
|---|---|
| `FREEZE_REPORT.md` | `77a0762582364b3c77ca78720e8feca9c2b44c3cbdf40b4a91037ca704064e8e` |
| `P0_W1A_ACCEPTANCE.md` | `cfd25a6064aa1c5fd3ad06558c43f79c2169ac88f7b80bd9dab05a90f739d249` |
| `P0_W1A_CASES.tsv` | `77af0924a86cf023a2924075507545b52035739e8c5bfc33accc080e8f4a9b17` |
| `verify_p0_w1a_successor.sh` | `35dfbedc0373f5292d96d9e0ab2feafbc11b3f35618adcaa2d5c921514304550` |

The canonical four-file package manifest is the SHA-256 of the
lexicographically ordered `sha256sum *` stream containing only those four
files:

```text
73cd9f5d4de76b618d3f483e957755271f81cfb503d48a63c4d4cdddbbfc2dc6
```

## Canonical 1dbbf scanner provenance

- oracle commit: `1dbbf3d206540d36f5912eab8372a42819778b47`
- source path: `packages/lix/tests/forktree_stage2_execution_oracle/main.rs`
- source git blob: `ae9a04ad4f6f87ee9bbd9c327382ad55b7a2ff1a`
- source SHA-256: `f71e91fcbccbb7d6df676a95e9d747725856b77f7e3177ec42f12ca8b28736cc`
- executable path: `/root/repos/lix-evidence/stage2-production-review-a12b/deletion/residue-oracle`
- executable SHA-256: `2aaf81d937110b5a248621420f0b3cbc7b5a116da8fbec0bb66453dde4e91585`
- exact baseline worktree/cwd: `/root/repos/lix-stage2-production-review-a1cf`
- exact baseline head: `a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`
- exact baseline tree: `d8326da2b1d38bd51b8ac7229d00684a6865bce2`
- transport worktree: `/root/repos/lix-stage2-p0-w1a-transport`

Exact separated-stream command:

```sh
cd /root/repos/lix-stage2-production-review-a1cf
/root/repos/lix-evidence/stage2-production-review-a12b/deletion/residue-oracle \
  audit /root/repos/lix-stage2-production-review-a1cf \
  >scanner.stdout 2>scanner.stderr
```

Expected exit status is 1 because this non-runnable baseline retains 166
classified findings. Captured identities are:

- semantic findings stdout: 8,351 bytes, 167 lines,
  `6f4013daca11867c9e07fab14b741c1650515eed473f87c12377e3421db8c42b`
- diagnostic stderr: 51 bytes, one line,
  `f6cd33b2a34a17e26eeccecb3ffef86210e9ff36595ad522433f5be8aa138907`
- exact `>combined 2>&1` stream:
  `3891a48613e5d6ebd3d0ab2780aed13c6dd0236f1c2ff343320dd73fb2158a0d`

The discrepancy is fully reconciled: `6f4013...` is stdout alone; `3891a486...`
is stdout followed by the failure diagnostic from stderr under the exact merged
redirection used by the frozen verifier. The scanner emits no checkout path in
either stream, so no path substitution was performed or required; normalizing
the findings means sorting/retaining its already canonical tab-separated stdout
rows plus `finding_count=166`, which is byte-identical to raw stdout and has
SHA-256 `6f4013...`.

R5/H3 must treat `6f4013...` plus finding count/per-key rows as the semantic
scanner evidence. The raw merged-stream hash `3891a486...` is retained only to
explain the unchanged frozen verifier and must not be compared across different
stdout/stderr capture arrangements. The four frozen package bytes were not
changed to retrofit this clarification.
